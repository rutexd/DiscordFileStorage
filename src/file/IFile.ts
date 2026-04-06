import { randomBytes } from 'crypto';

export interface IFile {
    name: string; // name is used only for raw provider, not for webdav. Webdav uses paths.  
    size: number;
    chunks: IChunkInfo[]
    created: Date;
    modified: Date;
    iv: Buffer
    // uploaded: boolean;
    encrypted: boolean;
    hmac?: Uint8Array; // HMAC-SHA256 tag for CTR mode integrity verification
}

export interface IChunkInfo {
    id: string; // discord (or any other provider) message id
    size: number;
    // url: string;
}


export type IFilesDesc = Record<string, IFile>;


/**
     * Returns file struct, no remote operations are done.
     */
export function createVFile(name: string, encrypted: boolean): IFile {
    return {
        name,
        size: 0,
        chunks: [],
        created: new Date(),
        modified: new Date(),
        encrypted,
        iv: encrypted ? randomBytes(16) : Buffer.alloc(0)
    };
}