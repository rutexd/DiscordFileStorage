import path from "path";
import BaseProvider from "./BaseProvider.js";
import HttpStreamPool from '../HttpStreamPool.js';

import { TextChannel } from "discord.js";
import { Writable, Readable } from "stream";
import { truncate } from "../helper/utils.js";
import { IFile } from "../file/IFile.js";

import Log from "../Log.js";
import DiscordHttpClient from "../discord/DiscordHttpClient.js";
import ChunkedUploadStream from "../discord/ChunkedUploadStream.js";

export const MAX_MB_CHUNK_SIZE = 10; // megabytes chunk size. Discord allows 10MB per file.

export const MAX_CHUNK_SIZE = MAX_MB_CHUNK_SIZE * 1000 * 1000;

export default class DiscordFileProvider extends BaseProvider {
    private httpClient: DiscordHttpClient | null = null;

    private getHttpClient(): DiscordHttpClient {
        if (!this.httpClient) {
            const token = this.client.getDiscordClient().token;
            if (!token) {
                throw new Error("Discord client not logged in - no token available");
            }
            const channelId = this.client.getFilesChannel().id;
            this.httpClient = new DiscordHttpClient(token, channelId);
        }
        return this.httpClient;
    }

    public async createRawReadStream(file: IFile): Promise<Readable> {
        Log.info(".createRawReadStream() - file: " + file.name);
        const channel = this.client.getFilesChannel() as TextChannel;
        
        return (await (new HttpStreamPool(file).getDownloadStream(async (id) => {
            // Since discords urls may expire, we need to request new ones.
            Log.info(`Resolving attachment URL for message: ${id}`);
            const message = await channel.messages.fetch(id);
            const attachment = message.attachments.first();
            if (!attachment) {
                throw new Error(`Message ${id} has no attachments`);
            }
            return attachment.url;
        })));
    }

    public async createRawWriteStream(file: IFile): Promise<Writable> {
        Log.info(".createRawWriteStream() - file: " + file.name);
        
        const httpClient = this.getHttpClient();
        const fileName = path.parse(truncate(file.name, 15)).name;
        const baseName = `${fileName}${file.encrypted ? ".enc" : ""}`;

        const chunkedUpload = new ChunkedUploadStream(httpClient, MAX_CHUNK_SIZE, baseName);

        const writeStream = chunkedUpload.createWriteStream();

        writeStream.on("finish", () => {
            const chunks = chunkedUpload.getChunkInfos();
            file.chunks.push(...chunks);
            Log.info(`[${file.name}] Upload complete: ${chunks.length} chunks, ${chunkedUpload.getTotalUploadedBytes()} bytes uploaded`);
        });

        return writeStream;
    }

    public async processDeletionQueue(): Promise<void> {
        if (this.deletionQueue.length > 0) {
            const info = this.deletionQueue.shift()!;
            
            try {
                const channel = this.client.getFilesChannel() as TextChannel;
                await channel.messages.delete(info.message);
                Log.info(`[DiscordProvider] Deleted message ${info.message}`);
            } catch (e) {
                Log.error(`[DiscordProvider] Failed to delete message ${info.message}:`, e);
            }
        }
    }

    /**
     * Drains the entire deletion queue synchronously.
     * Called during application shutdown to clean up all pending deletions.
     * 
     * Processes all entries sequentially without rate limiting since it's shutdown.
     */
    public async drainDeletionQueue(): Promise<void> {
        Log.info(`[DiscordProvider] Draining deletion queue (${this.deletionQueue.length} items)...`);
        
        while (this.deletionQueue.length > 0) {
            await this.processDeletionQueue();
        }
        
        Log.info("[DiscordProvider] Deletion queue drained.");
    }
}
