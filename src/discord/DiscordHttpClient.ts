import client from "../helper/AxiosInstance.js";
import Log from "../Log.js";
import FormData from "form-data";
import { Readable } from "stream";
import { userAgent } from "../Strings.js";

export interface DiscordAttachment {
    id: string;
    filename: string;
    size: number;
    url: string;
    proxy_url: string;
    content_type?: string;
}

export interface DiscordMessage {
    id: string;
    channel_id: string;
    attachments: DiscordAttachment[];
    timestamp: string;
}

export interface UploadResult {
    messageId: string;
    attachmentId: string;
    size: number;
    url: string;
}


// Discord.js is too complex for simple tasks and have overhead, so its simplier to use something we can control more fine.
export default class DiscordHttpClient {
    private botToken: string;
    private channelId: string;
    private baseUrl = "https://discord.com/api/v10";

    constructor(botToken: string, channelId: string) {
        this.botToken = botToken;
        this.channelId = channelId;
    }

    private getHeaders(): Record<string, string> {
        return {
            "Authorization": `Bot ${this.botToken}`,
            "User-Agent": userAgent,
        };
    }

    /**
     * Upload data to Discord as a message attachment with zero copy streaming.
     * 
     * @param data - File data as Buffer or Readable stream
     * @param filename - Name of the file attachment  
     * @returns Upload result with message ID and attachment info
     */
    public async uploadFile(data: Buffer | Readable, filename: string): Promise<UploadResult> {
        const url = `${this.baseUrl}/channels/${this.channelId}/messages`;

        const form = new FormData();
        // No knownLength = Transfer-Encoding: chunked
        form.append("files[0]", data, {
            filename,
            contentType: "application/octet-stream",
        });

        try {
            const response = await client.post<DiscordMessage>(url, form, {
                headers: {
                    ...this.getHeaders(),
                    ...form.getHeaders(),
                    "Transfer-Encoding": "chunked",
                },
                maxBodyLength: Infinity,
                maxContentLength: Infinity,
            });

            const message = response.data;
            const attachment = message.attachments[0];

            if (!attachment) {
                throw new Error("No attachment in response");
            }

            Log.info(`[DiscordHttpClient] Uploaded: ${filename} (${attachment.size} bytes) -> message ${message.id}`);

            return {
                messageId: message.id,
                attachmentId: attachment.id,
                size: attachment.size,
                url: attachment.url,
            };
        } catch (error: any) {
            Log.error(`[DiscordHttpClient] Upload failed:`, error.response?.data || error.message);
            throw error;
        }
    }
}
