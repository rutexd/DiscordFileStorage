import { createClient, createVFile } from '../index.js';

const client = await createClient({
    token: process.env.TOKEN || '...',
    guildId: process.env.GUILD_ID || '...',
    filesChannelName: "files-testcase",
    metaChannelName: "meta-testcase",
    shouldEncrypt: false,
});

const fs = client.getFs();

console.log('📦 DICloud initialized\n');

// =============================================================================
// 1. BUFFERED UPLOAD & DOWNLOAD
// =============================================================================
console.log('1️⃣  Buffered Operations:');

// Upload a file from buffer
const file1 = await client.uploadFile(Buffer.from('Hello from DICloud!'), 'greeting.txt');
fs.setFile('/greeting.txt', file1);

// Download file to buffer
const downloaded = await client.downloadFile(file1);
console.log('   Downloaded:', downloaded.toString());
console.log();

// =============================================================================
// 2. STREAMING UPLOAD & DOWNLOAD
// =============================================================================
console.log('2️⃣  Streaming Operations:');

// Create file metadata
const streamFile = createVFile('large-data.txt', 0, false);

// Upload via stream
const writeStream = await client.createWriteStream(streamFile);
writeStream.write('Chunk 1: ');
writeStream.write('Chunk 2: ');
writeStream.end('Final chunk!');

await new Promise((resolve, reject) => {
    writeStream.on('finish', resolve);
    writeStream.on('error', reject);
});

fs.setFile('/large-data.txt', streamFile);
console.log('   Uploaded via stream:', streamFile.size, 'bytes');

// Download via stream
const readStream = await client.createReadStream(streamFile);
let streamContent = '';
for await (const chunk of readStream) {
    streamContent += chunk.toString();
}
console.log('   Downloaded via stream:', streamContent);
console.log();

// =============================================================================
// 3. PERSISTENCE TEST
// =============================================================================
console.log('3️⃣  Persistence:');

const counterPath = '/run-counter.txt';

// Check if counter exists from previous run
if (fs.existsSync(counterPath)) {
    const counterFile = fs.getFile(counterPath);
    const count = parseInt((await client.downloadFile(counterFile)).toString()) + 1;
    console.log('   Run #' + count + ' (loaded from previous session)');
    
    // Update counter (old chunks auto-deleted)
    const updated = await client.uploadFile(Buffer.from(count.toString()), 'run-counter.txt');
    fs.setFile(counterPath, updated);
} else {
    console.log('   Run #1 (first run)');
    const counter = await client.uploadFile(Buffer.from('1'), 'run-counter.txt');
    fs.setFile(counterPath, counter);
}

// Save metadata (persists between restarts)
client.markForUpload();
await new Promise(resolve => setTimeout(resolve, 2500));

console.log('\n✅ All operations complete!');
console.log('💡 Run this script again to see persistence in action.');

// Graceful shutdown (cleans up old chunks)
await client.shutdown();
