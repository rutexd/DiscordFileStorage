import path from "path";
import BaseProvider from "./BaseProvider.js";
import HttpStreamPool from '../HttpStreamPool.js';

import { AttachmentBuilder, TextChannel } from "discord.js";
import { Writable, Readable, PassThrough } from "stream";
import { truncate } from "../helper/utils.js";
import { IFile } from "../file/IFile.js";
import MutableBuffer from "../helper/MutableBuffer.js";

import Log from "../Log.js";

export const MAX_MB_CHUNK_SIZE = 10; // megabytes chunk size. Discord allows 10MB per file.
export const ENCRYPTION_OVERHEAD = 16; // 16 bytes for encryption metadata

export const MAX_CHUNK_SIZE = (MAX_MB_CHUNK_SIZE * 1000 * 1000) - ENCRYPTION_OVERHEAD;

/**
 * Splits a buffer into chunks of maximum size.
 */
function splitBufferBy(buffer: Buffer, chunkSize: number): Buffer[] {
    const chunks: Buffer[] = [];
    for (let i = 0; i < buffer.length; i += chunkSize) {
        chunks.push(buffer.subarray(i, i + chunkSize));
    }
    return chunks;
}

export default class DiscordFileProvider extends BaseProvider {
    /**
     * Creates a readable stream that downloads file chunks from Discord.
     * Each chunk is downloaded sequentially via HTTP URL.
     * 
     * @param file - File metadata with chunk information
     * @returns A readable stream of file data
     */
    public async createRawReadStream(file: IFile): Promise<Readable> {
        Log.info(".createRawReadStream() - file: " + file.name);
        return (await (new HttpStreamPool(file).getDownloadStream(async (id) => {
            return (await this.client.getFilesChannel().messages.fetch(id)).attachments.first()!.url;
        })));
    }

    /**
     * Uploads a chunk to Discord and records metadata.
     */
    private async uploadChunkToDiscord(
        chunkBuffer: MutableBuffer,
        chunkId: number,
        channel: TextChannel,
        file: IFile,
        fileName: string
    ): Promise<void> {
        try {
            Log.info(`[${file.name}] Uploading chunk ${chunkId} (${chunkBuffer.size} bytes)...`);
            
            const message = await channel.send({
                files: [
                    new AttachmentBuilder(Readable.from([chunkBuffer.buffer.subarray(0, chunkBuffer.size)]), {
                        name: `${chunkId}-${fileName}${file.encrypted ? ".enc" : ""}`
                    })
                ]
            });

            file.chunks.push({
                id: message.id,
                size: chunkBuffer.size
            });
            
            Log.info(`[${file.name}] Chunk ${chunkId} uploaded (message: ${message.id})`);
        } catch (error) {
            Log.error(`[${file.name}] Failed to upload chunk ${chunkId}:`, error);
            throw error;
        }
    }

    /**
     * Creates a writable stream that uploads file data to Discord in 10MB chunks.
     * 
     * Implementation:
     * - Uses MutableBuffer to accumulate encrypted data
     * - Uploads chunks when they reach Discord's 10MB limit
     * - Handles overflow by splitting and uploading, then starting new buffer
     * 
     * @param file - File metadata object (updated with chunks as they're uploaded)
     * @returns A writable stream that handles file upload
     */
    public async createRawWriteStream(file: IFile): Promise<Writable> {
        Log.info(".createRawWriteStream() - file: " + file.name);
        
        const channel = this.client.getFilesChannel();
        const fileName = path.parse(truncate(file.name, 15)).name;
        let chunkId = 1;
        let chunkBuffer = new MutableBuffer();
        let totalFileSize = 0;
        
        // Discord's 10MB file size limit (plaintext + encryption overhead)
        const discordLimit = MAX_CHUNK_SIZE + ENCRYPTION_OVERHEAD;

        const uploadStream = new Writable({
            highWaterMark: 256 * 1024,
            write: async (chunk_: Buffer, encoding: BufferEncoding, callback) => {
                try {
                    // Add chunk data in smaller pieces to avoid huge temp buffers
                    let remaining = chunk_;
                    while (remaining.length > 0) {
                        const space = discordLimit - chunkBuffer.size;
                        if (space <= 0) {
                            // Current buffer is full, upload it
                            await this.uploadChunkToDiscord(chunkBuffer, chunkId, channel, file, fileName);
                            chunkBuffer.destroy();
                            chunkBuffer = new MutableBuffer();
                            chunkId++;
                            continue;
                        }
                        
                        // Write what fits into current buffer
                        const toWrite = Math.min(remaining.length, space);
                        chunkBuffer.write(remaining.subarray(0, toWrite));
                        totalFileSize += toWrite;
                        remaining = remaining.subarray(toWrite);
                    }
                    callback();
                } catch (error) {
                    callback(error instanceof Error ? error : new Error(String(error)));
                }
            },
            final: async (callback) => {
                try {
                    Log.info(`[${file.name}] Finalizing upload.`);
                    if (chunkBuffer.size > 0) {
                        await this.uploadChunkToDiscord(chunkBuffer, chunkId, channel, file, fileName);
                        chunkBuffer.destroy();
                    }
                    Log.info(`[${file.name}] Upload complete. Total size: ${totalFileSize} bytes, chunks: ${chunkId - 1}`);
                    callback();
                } catch (error) {
                    callback(error instanceof Error ? error : new Error(String(error)));
                }
            }
        });
    
        return uploadStream;
    }

    /**
     * Processes a single deletion from the queue.
     * Called by the tick loop to gradually drain the deletion queue.
     * 
     * Implements rate-limiting by processing one deletion per tick interval,
     * preventing Discord API rate limiting.
     */
    public async processDeletionQueue(): Promise<void> {
        if (this.deletionQueue.length > 0) {
            const info = this.deletionQueue.shift()!;
            const channel = this.client.getDiscordClient().channels.cache.get(info.channel) as TextChannel;

            if (!channel) {
                Log.error(`[DiscordProvider] Failed to find channel: ${info.channel}`);
                return;
            }
            try {
                await channel.messages.delete(info.message);
                Log.info(`[DiscordProvider] Deleted message ${info.message} from channel ${info.channel}`);
            } catch (e) {
                Log.error(`[DiscordProvider] Failed to delete message ${info.message} from channel ${info.channel}:`, e);
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
            const info = this.deletionQueue.shift()!;
            const channel = this.client.getDiscordClient().channels.cache.get(info.channel) as TextChannel;

            if (!channel) {
                Log.error(`[DiscordProvider] Failed to find channel: ${info.channel}`);
                continue;
            }
            try {
                await channel.messages.delete(info.message);
                Log.info(`[DiscordProvider] Deleted message ${info.message}`);
            } catch (e) {
                Log.error(`[DiscordProvider] Failed to delete message ${info.message}:`, e);
            }
        }
        Log.info("[DiscordProvider] Deletion queue drained.");
    }

    calculateProviderMaxSize(): number {
        return MAX_CHUNK_SIZE;
    }

    calculateSavedFileSize(): number {
        return MAX_CHUNK_SIZE + ENCRYPTION_OVERHEAD;
    }
}
