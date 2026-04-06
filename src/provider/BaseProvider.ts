import DICloudApp from "../DICloudApp";
import MutableBuffer from "../helper/MutableBuffer";

import { createVFile, IFile } from "../file/IFile";
import { PassThrough, Readable, Transform, Writable } from "stream";
import { gcm } from '@noble/ciphers/aes';
import { Cipher, utf8ToBytes } from '@noble/ciphers/utils';
import { ensureStringLength, withResolvers } from "../helper/utils";

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


    private createCipher(iv: Uint8Array): Cipher {
        const password = this.client.getEncryptPassword();
        if (!password) {
            throw new Error("Encryption password is required to process encrypted files.");
        }

        let key = utf8ToBytes(password);
        if (key.length !== 16 && key.length !== 24 && key.length !== 32) {
            // Keep compatibility with server mode which normalizes to 32 chars.
            key = utf8ToBytes(ensureStringLength(password, 32));
        }

        Log.info("[BaseProvider] Creating cipher with key length:", key.length, "password length:", password.length, "IV:", Array.from(iv).slice(0, 4).join(","));

        return gcm(key, iv);
    }

    private createCipherWithIV(iv: Uint8Array): Cipher {
        const password = this.client.getEncryptPassword();
        if (!password) {
            throw new Error("Encryption password is required to process encrypted files.");
        }

        let key = utf8ToBytes(password);
        if (key.length !== 16 && key.length !== 24 && key.length !== 32) {
            key = utf8ToBytes(ensureStringLength(password, 32));
        }

        return gcm(key, iv);
    }

    private deriveBlockIV(baseIV: Uint8Array, blockNumber: number): Uint8Array {
        const derivedIV = new Uint8Array(baseIV);
        const view = new DataView(derivedIV.buffer);
        
        const currentValue = view.getUint32(12, true);
        view.setUint32(12, currentValue + blockNumber, true);
        
        return derivedIV;
    }

    private async createReadStreamWithDecryption(file: IFile): Promise<Readable> {
        const readStream = await this.createRawReadStream(file);
        const decryptedRead = new PassThrough();
        const encryptedChunkSize = this.calculateSavedFileSize();
        const buffer = new MutableBuffer(encryptedChunkSize);
        let blockNumber = 0;
        let outputSize = 0;

        readStream.on("data", (chunk) => {
            try {
                const left = encryptedChunkSize - buffer.size;

                if (chunk.length <= left) {
                    buffer.write(chunk);
                } else {
                    buffer.write(chunk.subarray(0, left));
                    const blockIV = this.deriveBlockIV(file.iv, blockNumber);
                    const decipher = this.createCipherWithIV(blockIV);
                    const decrypted = decipher.decrypt(buffer.cloneNativeBuffer());
                    
                    // Only output what's needed (trim padding)
                    const remaining = file.size - outputSize;
                    const toOutput = Math.min(decrypted.length, remaining);
                    if (toOutput > 0) {
                        const writeSuccess = decryptedRead.write(decrypted.subarray(0, toOutput));
                        outputSize += toOutput;
                        if (!writeSuccess) {
                            readStream.pause();
                        }
                    }
                    
                    buffer.clear();
                    buffer.write(chunk.subarray(left));
                    blockNumber++;
                }
            } catch (err) {
                decryptedRead.destroy(err instanceof Error ? err : new Error(String(err)));
                buffer.destroy();
            }
        });

        readStream.on("end", () => {
            try {
                if (buffer.size > 0) {
                    const blockIV = this.deriveBlockIV(file.iv, blockNumber);
                    const decipher = this.createCipherWithIV(blockIV);
                    const decrypted = decipher.decrypt(buffer.cloneNativeBuffer());
                    
                    // Only output remaining plaintext
                    const remaining = file.size - outputSize;
                    const toOutput = Math.min(decrypted.length, remaining);
                    if (toOutput > 0) {
                        decryptedRead.write(decrypted.subarray(0, toOutput));
                    }
                }
                buffer.destroy();
                decryptedRead.end();
            } catch (err) {
                decryptedRead.destroy(err instanceof Error ? err : new Error(String(err)));
                buffer.destroy();
            }
        });

        decryptedRead.on("drain", () => {
            readStream.resume();
        });

        readStream.on("error", (err) => {
            decryptedRead.destroy(err);
            buffer.destroy();
        });

        decryptedRead.on("error", (err) => {
            readStream.destroy(err);
            buffer.destroy();
        });

        return decryptedRead;
    }


    private async createWriteStreamWithEncryption(file: IFile): Promise<Writable> {
        const rawWriteStream = await this.createRawWriteStream(file);
        const writeStreamAwaiter = withResolvers();
        let buffer = new MutableBuffer(this.calculateProviderMaxSize());
        let blockNumber = 0;
        let plainTextSize = 0;

        rawWriteStream.on("finish", () => {
            writeStreamAwaiter.resolve();
        });

        rawWriteStream.on("error", (err) => {
            writeStreamAwaiter.reject(err);
            buffer.destroy();
        });

        return new Writable({
            write: async (chunk: Buffer, encoding, callback) => {
                const left = this.calculateProviderMaxSize() - buffer.size;
                if (chunk.length <= left) {
                    buffer.write(chunk, encoding);
                    plainTextSize += chunk.length;
                } else {
                    buffer.write(chunk.subarray(0, left), encoding);
                    plainTextSize += left;
                    
                    const blockIV = this.deriveBlockIV(file.iv, blockNumber);
                    const cipher = this.createCipherWithIV(blockIV);
                    const encrypted = cipher.encrypt(buffer.cloneNativeBuffer());
                    rawWriteStream.write(encrypted);
                    
                    buffer.clear();
                    buffer.write(chunk.subarray(left), encoding);
                    plainTextSize += chunk.length - left;
                    blockNumber++;
                }
                callback();
            },
            final: async (callback) => {
                Log.info("[BaseProvider] final() Finalizing upload.");
                if (buffer.size > 0) {
                    const blockIV = this.deriveBlockIV(file.iv, blockNumber);
                    const cipher = this.createCipherWithIV(blockIV);
                    const encrypted = cipher.encrypt(buffer.flushAndDestory());
                    rawWriteStream.write(encrypted);
                }
                // Set file size to plaintext size before closing
                file.size = plainTextSize;
                rawWriteStream.end();
                await writeStreamAwaiter.promise;
                callback();
            },
            destroy: (err, callback) => {
                Log.info("[BaseProvider] destroy() Destroying write stream (error: " + err + ")");
                buffer.destroy();
                callback(err);
            }
        });
    }



    public abstract processDeletionQueue(): Promise<void>;

    /**
     * Method that should drain the entire deletion queue (called during shutdown).
     * Default implementation calls processDeletionQueue() in a loop.
     */
    public async drainDeletionQueue(): Promise<void> {
        while (this.deletionQueue.length > 0) {
            await this.processDeletionQueue();
        }
    }

    public abstract createRawReadStream(file: IFile): Promise<Readable>;

    public abstract createRawWriteStream(file: IFile): Promise<Writable>;

    /**
     * Returns the maximum size a single chunk can be in the provider.
     * Used to determine encryption block boundaries.
     */
    abstract calculateProviderMaxSize(): number;

    /**
     * Returns the stored file size after encryption (includes overhead).
     * Used to align decryption buffers correctly.
     */
    abstract calculateSavedFileSize(): number;

    async createReadStream(file: IFile): Promise<Readable> {
        if (file.encrypted) {
            Log.info(`[BaseProvider] Creating encrypted read stream for "${file.name}"`);
            return await this.createReadStreamWithDecryption(file);
        }
        Log.info(`[BaseProvider] Creating raw read stream for "${file.name}"`);
        return await this.createRawReadStream(file);
    }

    async createWriteStream(file: IFile): Promise<Writable> {
        if (file.encrypted) {
            Log.info(`[BaseProvider] Creating encrypted write stream for "${file.name}"`);
            return await this.createWriteStreamWithEncryption(file);
        }
        Log.info(`[BaseProvider] Creating raw write stream for "${file.name}"`);
        return await this.createRawWriteStream(file);
    }

    public async uploadFile(buffer: Buffer, name: string): Promise<IFile> {
        const file = createVFile(name, 0, this.client.shouldEncryptFiles());
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

}