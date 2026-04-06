import Log from "../Log.js";
import { Writable, PassThrough } from "stream";
import DiscordHttpClient, { UploadResult } from "./DiscordHttpClient.js";
import { IChunkInfo } from "../file/IFile.js";

export interface ChunkUploadResult {
    chunkIndex: number;
    messageId: string;
    size: number;
}


export default class ChunkedUploadStream {
    private discordClient: DiscordHttpClient;
    private maxChunkSize: number;
    private baseFilename: string;
    
    private uploadedChunks: ChunkUploadResult[] = [];
    private totalUploadedBytes = 0;
    private isCancelled = false;

    constructor(
        discordClient: DiscordHttpClient,
        maxChunkSize: number,
        baseFilename: string
    ) {
        this.discordClient = discordClient;
        this.maxChunkSize = maxChunkSize;
        this.baseFilename = baseFilename;
    }

    public createWriteStream(): Writable {
        let currentSession: StreamingSession | null = null;
        let chunkIndex = 0;
        const pendingUploads: Promise<ChunkUploadResult>[] = [];

        const startNewSession = (): StreamingSession => {
            const filename = `${chunkIndex}-${this.baseFilename}`;
            Log.info(`[ChunkedUploadStream] Starting session ${chunkIndex}: ${filename}`);
            
            const session = new StreamingSession(
                this.discordClient,
                filename,
                this.maxChunkSize,
                chunkIndex
            );

            const uploadPromise = session.startUpload().then((result) => {
                this.uploadedChunks.push(result);
                this.totalUploadedBytes += result.size;
                Log.info(`[ChunkedUploadStream] Session ${result.chunkIndex} uploaded: ${result.size} bytes -> ${result.messageId}`);
                return result;
            });
            
            pendingUploads.push(uploadPromise);

            chunkIndex++;
            return session;
        };

        return new Writable({
            highWaterMark: 64 * 1024,

            write: async (data: Buffer, _encoding, callback) => {
                if (this.isCancelled) {
                    callback(new Error("Upload cancelled"));
                    return;
                }

                try {
                    let offset = 0;

                    while (offset < data.length) {
                        if (!currentSession) {
                            currentSession = startNewSession();
                        }

                        const available = currentSession.remainingCapacity();
                        const toWrite = Math.min(available, data.length - offset);
                        const slice = data.subarray(offset, offset + toWrite);

                        const canContinue = currentSession.write(slice);
                        offset += toWrite;

                        if (currentSession.remainingCapacity() === 0) {
                            await currentSession.end();
                            currentSession = null;
                        } else if (!canContinue) {
                            await currentSession.waitForDrain();
                        }
                    }

                    callback();
                } catch (error) {
                    callback(error instanceof Error ? error : new Error(String(error)));
                }
            },

            final: async (callback) => {
                try {
                    if (currentSession && currentSession.bytesWritten > 0) {
                        await currentSession.end();
                    } else if (currentSession) {
                        currentSession.abort();
                    }
                    
                    // Wait for ALL uploads to complete
                    await Promise.all(pendingUploads);
                    
                    Log.info(`[ChunkedUploadStream] Complete: ${this.totalUploadedBytes} bytes in ${this.uploadedChunks.length} chunks`);
                    callback();
                } catch (error) {
                    callback(error instanceof Error ? error : new Error(String(error)));
                }
            },

            destroy: (error, callback) => {
                this.isCancelled = true;
                currentSession?.abort();
                callback(error);
            }
        });
    }

    public getUploadedChunks(): ChunkUploadResult[] {
        return [...this.uploadedChunks];
    }

    public getChunkInfos(): IChunkInfo[] {
        return this.uploadedChunks.map(chunk => ({
            id: chunk.messageId,
            size: chunk.size,
        }));
    }

    public getTotalUploadedBytes(): number {
        return this.totalUploadedBytes;
    }

    public cancel(): void {
        this.isCancelled = true;
    }
}


class StreamingSession {
    private passthrough: PassThrough;
    private _bytesWritten = 0;
    private maxSize: number;
    private filename: string;
    private chunkIndex: number;
    private discordClient: DiscordHttpClient;
    private uploadPromise: Promise<ChunkUploadResult> | null = null;

    constructor(
        discordClient: DiscordHttpClient,
        filename: string,
        maxSize: number,
        chunkIndex: number
    ) {
        this.discordClient = discordClient;
        this.filename = filename;
        this.maxSize = maxSize;
        this.chunkIndex = chunkIndex;
        
        this.passthrough = new PassThrough({ highWaterMark: 64 * 1024 });
    }

    get bytesWritten(): number {
        return this._bytesWritten;
    }

    remainingCapacity(): number {
        return this.maxSize - this._bytesWritten;
    }

    write(data: Buffer): boolean {
        this._bytesWritten += data.length;
        return this.passthrough.write(data);
    }

    waitForDrain(): Promise<void> {
        return new Promise((resolve) => {
            this.passthrough.once("drain", resolve);
        });
    }

    startUpload(): Promise<ChunkUploadResult> {
        if (this.uploadPromise) {
            return this.uploadPromise;
        }

        this.uploadPromise = (async () => {
            const result = await this.discordClient.uploadFile(
                this.passthrough,
                this.filename
            );

            return {
                chunkIndex: this.chunkIndex,
                messageId: result.messageId,
                size: result.size,
            };
        })();

        return this.uploadPromise;
    }

    async end(): Promise<ChunkUploadResult> {
        this.passthrough.end();
        
        if (!this.uploadPromise) {
            throw new Error("Session was never started");
        }

        return this.uploadPromise;
    }

    abort(): void {
        this.passthrough.destroy();
    }
}
