import { DirectoryJSON, Volume } from "memfs";
import { IFile } from "./IFile.js";

export interface IEntry {
    file: boolean,
    name: string,
}

export default class VolumeEx extends Volume {
    private cleanupCallback?: (chunks: Array<{id: string, size: number}>) => void;

    public static fromJSON(json: DirectoryJSON, cwd?: string | undefined): VolumeEx {
        const vol = new VolumeEx(cwd);
        vol.fromJSON(json);
        return vol;
    }

    /**
     * Set cleanup callback for automatic chunk deletion when files are replaced.
     * Called by DICloudApp during initialization.
     */
    public setCleanupCallback(callback: (chunks: Array<{id: string, size: number}>) => void): void {
        this.cleanupCallback = callback;
    }

    public pathExists(path: string): boolean {
        try {
            this.statSync(path);
            return true;
        } catch (e) {
            return false;
        }
    }
    
    public getFile(path: string): IFile {
        return JSON.parse(this.readFileSync(path).toString(), (k, v) => {
            if (k === "created" || k === "modified") {
                return new Date(v);
            }
            if (k === "iv") {
                return new Uint8Array(Object.values(v));
            }

            return v;
        }) as IFile;
    }

    public setFile(path: string, file: IFile) {
        // Automatic cleanup: if file exists, queue old chunks for deletion
        if (this.cleanupCallback && this.existsSync(path)) {
            try {
                const oldFile = this.getFile(path);
                if (oldFile.chunks && oldFile.chunks.length > 0) {
                    this.cleanupCallback(oldFile.chunks);
                }
            } catch (e) {
                // File might not be a valid IFile, ignore
            }
        }
        
        this.writeFileSync(path, JSON.stringify(file));
    }

    private getFilesPathsRecursive(initial: string, paths: string[] = []) {
        const entries = this.readdirSync(initial, { withFileTypes: true });
        for (const entry of entries) {
            const path = initial === '/' ? '/' + (entry as any).name : initial + '/' + (entry as any).name;
            if ((entry as any).isDirectory()) {
                this.getFilesPathsRecursive(path, paths);
            } else {
                paths.push(path);
            }
        }
        return paths;
    }

    public getFilesRecursive(path: string): IFile[] {
        return this.getFilesPathsRecursive(path).map(p => this.getFile(p));
    }

    public getFilesWithPathRecursive(path: string): Record<string, IFile> {
        return this.getFilesPathsRecursive(path).reduce((acc, path) => {
            acc[path] = this.getFile(path);
            return acc;
        }, {} as Record<string, IFile>);
    }

    public getPathsRecursive(path: string): string[] {
        return this.getFilesPathsRecursive(path);
    }

    public getTreeSizeRecursive(path: string): number {
        return this.getFilesRecursive(path).reduce((acc, file) => acc + file.size, 0);
    }

    // todo: reorganize this
    public getFilesAndFolders(path: string): IEntry[] {
        return this.readdirSync(path, { withFileTypes: true }).map((entry) => {
            return {
                file: (entry as any).isFile(),
                name: (entry as any).name.toString(),
            }
        });
    }

        
    
    

}