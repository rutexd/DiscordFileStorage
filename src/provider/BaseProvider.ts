import DICloudApp from "../DICloudApp";

import { createVFile, IFile } from "../file/IFile";
import { Readable, Transform, Writable } from "stream";
import { withResolvers } from "../helper/utils";
import { createCipheriv, createDecipheriv } from "crypto";
import { deriveKey, createHmacSha256 } from "../helper/Crypto.js";

import Log from "../Log";
export interface IDelayedDeletionEntry {
    channel: string;
    message: string;
}

export default abstract class BaseProvider {
    private _app: DICloudApp;
    private fileDeletionQueue: Array<IDelayedDeletionEntry> = [];

    public constructor(app: DICloudApp) {
        this._app = app;
    }

    public get client() {
        return this._app;
    }

    public addToDeletionQueue(info: IDelayedDeletionEntry[]) {
        this.fileDeletionQueue.push(...info);
        Log.info(`[BaseProvider] Added ${info.length} entries to deletion queue (total: ${this.fileDeletionQueue.length})`);
    }

    public get deletionQueue() {
        return this.fileDeletionQueue;
    }

    public abstract processDeletionQueue(): Promise<void>;
    public abstract createRawReadStream(file: IFile): Promise<Readable>;
    public abstract createRawWriteStream(file: IFile): Promise<Writable>;


    private async createReadStreamWithDecryption(file: IFile): Promise<Readable> {
        const password = this.client.getEncryptPassword();
        if (!password) {
            throw new Error("Encryption password is required to process encrypted files.");
        }
        
        const readStream = await this.createRawReadStream(file);
        const key = deriveKey(password);
        const iv = file.iv;
        
        const decipher = createDecipheriv('aes-256-ctr', key as any, iv as any);
        const hmac = createHmacSha256(key);
        
        let outputSize = 0;
        const expectedSize = file.size;
        const expectedHmac = file.hmac;

        const decryptTransform = new Transform({
            transform(chunk: Buffer, encoding, callback) {
                try {
                    hmac.update(chunk as any);                    
                
                    const decrypted = decipher.update(chunk as any);
                
                    const remaining = expectedSize - outputSize;
                    if (remaining <= 0) {
                        callback();
                        return;
                    }
                    
                    const toOutput = Math.min(decrypted.length, remaining);
                    outputSize += toOutput;
                    
                    callback(null, decrypted.subarray(0, toOutput));
                } catch (err) {
                    callback(err instanceof Error ? err : new Error(String(err)));
                }
            },
            
            flush(callback) {
                try {
                    decipher.final();
                    
                    const computedHmac = hmac.digest();
                    
                    if (expectedHmac && expectedHmac.length > 0) {
                        const expected = Buffer.from(expectedHmac);
                        if (!computedHmac.equals(expected as any)) {
                            callback(new Error("HMAC verification failed - data may be corrupted or tampered"));
                            return;
                        }
                        Log.info("[BaseProvider] HMAC verification passed");
                    } else {
                        Log.warn("[BaseProvider] No HMAC stored - skipping integrity check (legacy file?)");
                    }
                    
                    callback();
                } catch (err) {
                    callback(err instanceof Error ? err : new Error(String(err)));
                }
            }
        });

        readStream.on("error", (err) => decryptTransform.destroy(err));
        decryptTransform.on("error", (err) => readStream.destroy(err));

        return readStream.pipe(decryptTransform);
    }

    private async createWriteStreamWithEncryption(file: IFile): Promise<Writable> {
        const password = this.client.getEncryptPassword();
        if (!password) {
            throw new Error("Encryption password is required to process encrypted files.");
        }
        
        const rawWriteStream = await this.createRawWriteStream(file);
        const writeStreamAwaiter = withResolvers();
        const key = deriveKey(password);
        const iv = file.iv;
        
        const cipher = createCipheriv('aes-256-ctr', key as any, iv as any);
        const hmac = createHmacSha256(key);
        let plainTextSize = 0;

        rawWriteStream.on("finish", () => {
            writeStreamAwaiter.resolve();
        });

        rawWriteStream.on("error", (err) => {
            writeStreamAwaiter.reject(err);
        });

        return new Writable({
            write: (chunk: Buffer, encoding, callback) => {
                try {
                    plainTextSize += chunk.length;
                    const encrypted = cipher.update(chunk as any);
                    hmac.update(encrypted as any);
                    const canContinue = rawWriteStream.write(encrypted);
                    
                    if (!canContinue) {
                        rawWriteStream.once('drain', callback);
                    } else {
                        callback();
                    }
                } catch (err) {
                    callback(err instanceof Error ? err : new Error(String(err)));
                }
            },
            
            final: async (callback) => {
                try {
                    Log.info("[BaseProvider] final() Finalizing CTR+HMAC upload.");
                    
                    // Finalize cipher (CTR doesn't produce extra output, but good practice)
                    const finalBlock = cipher.final();
                    if (finalBlock.length > 0) {
                        hmac.update(finalBlock as any);
                        rawWriteStream.write(finalBlock);
                    }
                    
                    file.hmac = new Uint8Array(hmac.digest());
                    file.size = plainTextSize;
                    
                    Log.info(`[BaseProvider] HMAC computed: ${Buffer.from(file.hmac).toString('hex').slice(0, 16)}...`);
                    
                    rawWriteStream.end();
                    await writeStreamAwaiter.promise;
                    callback();
                } catch (err) {
                    callback(err instanceof Error ? err : new Error(String(err)));
                }
            },
            
            destroy: (err, callback) => {
                Log.info("[BaseProvider] destroy() Destroying write stream (error: " + err + ")");
                rawWriteStream.destroy(err || undefined);
                callback(err);
            }
        });
    }




    async createReadStream(file: IFile): Promise<Readable> {
        if (file.encrypted) {
            Log.info(`[BaseProvider] Creating encrypted read stream for "${file.name}"`);
            return this.createReadStreamWithDecryption(file);
        }
        Log.info(`[BaseProvider] Creating raw read stream for "${file.name}"`);
        return this.createRawReadStream(file);
    }

    async createWriteStream(file: IFile): Promise<Writable> {
        if (file.encrypted) {
            Log.info(`[BaseProvider] Creating encrypted write stream for "${file.name}"`);
            return this.createWriteStreamWithEncryption(file);
        }
        Log.info(`[BaseProvider] Creating raw write stream for "${file.name}"`);
        return this.createRawWriteStream(file);
    }



    public async downloadFile(file: IFile): Promise<Buffer> {
        const stream = await this.createReadStream(file);

        return new Promise((resolve, reject) => {
            const buffers: Buffer[] = [];
            let totalSize = 0;

            stream.on("data", (chunk: Buffer) => {
                buffers.push(chunk);
                totalSize += chunk.length;
            });

            stream.on("end", () => {
                Log.info(`[BaseProvider] downloadFile("${file.name}") complete - ${totalSize} bytes`);
                resolve(Buffer.concat(buffers as any, totalSize));
            });

            stream.on("error", (err) => {
                Log.error(`[BaseProvider] downloadFile("${file.name}") failed:`, err);
                reject(err);
            });
        });
    }

    public async uploadFile(buffer: Buffer, name: string): Promise<IFile> {
        const file = createVFile(name, this.client.shouldEncryptFiles());
        const stream = await this.createWriteStream(file);

        // Set the plaintext size upfront so encrypted files preserve original size
        file.size = buffer.length;

        return new Promise(async (resolve, reject) => {
            stream.on("finish", () => {
                Log.info(`[BaseProvider] uploadFile("${name}") complete - ${file.size} bytes`);
                resolve(file);
            });

            stream.on("error", (err) => {
                Log.error(`[BaseProvider] uploadFile("${name}") failed:`, err);
                reject(err);
            });
            
            Readable.from(buffer).pipe(stream);
        });
    }



    /**
     * Method that should drain the entire deletion queue (called during proper shutdown).
     * Default implementation calls processDeletionQueue() in a loop.
     */
    public async drainDeletionQueue(): Promise<void> {
        while (this.deletionQueue.length > 0) {
            await this.processDeletionQueue();
        }
    }

}