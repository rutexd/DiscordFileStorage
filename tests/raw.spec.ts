import dotenv from 'dotenv';
dotenv.config();

import * as uvu from 'uvu';
import * as assert from 'uvu/assert';
import { createHash } from 'crypto';

import { envBoot } from "../bootloader"
import DICloudApp from '../src/DICloudApp';

const test = uvu.test;

let app: DICloudApp;

async function sleep(ms: number) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function hashBuffer(buffer: Buffer): string {
    return createHash('sha256').update(buffer as any).digest('hex');
}


test('server boot', async () => {
    app = await envBoot();
    assert.ok(app);
});


test("create and read small file", async () => {
    const data = {
        name: "test-small.txt",
        content: "Hello, World!",
        size: 0
    }
    data.size = Buffer.byteLength(data.content);

    const buffer = Buffer.from(data.content);
    const originalHash = hashBuffer(buffer);
    const uploadedFile = await app.uploadFile(buffer, data.name);
    assert.is(uploadedFile.size, data.size);

    const downloadedBuffer = await app.downloadFile(uploadedFile);
    const downloadedHash = hashBuffer(downloadedBuffer);
    assert.is(downloadedHash, originalHash, 'Hash mismatch: data corrupted during upload/download');
});

test("create and read big", async () => {
    const fileSize = 15_000_000; // 15MB

    const data = {
        name: "test-big.txt",
        content: Buffer.alloc(fileSize, 0).toString(),
        size: 0
    }
    data.size = Buffer.byteLength(data.content);

    const buffer = Buffer.from(data.content);
    const originalHash = hashBuffer(buffer);
    const uploadedFile = await app.uploadFile(buffer, data.name);
    assert.is(uploadedFile.size, data.size);

    const downloadedBuffer = await app.downloadFile(uploadedFile);
    const downloadedHash = hashBuffer(downloadedBuffer);
    assert.is(downloadedHash, originalHash, 'Hash mismatch: data corrupted during upload/download');
});

test("create and read empty file", async () => {
    const data = {
        name: "empty.txt",
        content: "",
        size: 0
    }
    data.size = Buffer.byteLength(data.content);

    const buffer = Buffer.from(data.content);
    const originalHash = hashBuffer(buffer);
    const uploadedFile = await app.uploadFile(buffer, data.name);
    assert.is(uploadedFile.size, data.size);

    const downloadedBuffer = await app.downloadFile(uploadedFile);
    const downloadedHash = hashBuffer(downloadedBuffer);
    assert.is(downloadedBuffer.toString(), data.content);
    assert.is(downloadedHash, originalHash, 'Hash mismatch: data corrupted during upload/download');
});



test.run();