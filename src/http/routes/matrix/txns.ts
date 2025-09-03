import { http, services, utils } from "@varius.io/framework";
import * as SparkPlugins from "#src/models/spark-plugins";
import axios from "axios";
import { Communities } from "#src/models";
import { putFirehoseEvent } from "#src/utils/firehose";

const crypto = require("crypto").webcrypto;

const { MATRIX_DOMAIN } = process.env

export async function sendEventToFirehose(accessToken: string, businessId: string, userId: string, event: any) {
    try {

        if (!event.room_id) return

        utils.logger.info("Called Matrix Server: getRoomAliases");
        const [ alias ] = await services.matrix.getRoomAliases(accessToken, event.room_id)
        utils.logger.info("Called Matrix Server: getRoomAliases");
        if (!alias) return

        // `${space.businessId}.${space.id}
        const spaceId = alias.split(":")[0].split(".")[1]
        if (!spaceId) return

        utils.logger.info("Called Matrix Server: GET community");
        const community = await Communities.getByChildId(event.room_id);
        utils.logger.info("Called Matrix Server: GET community");

        const eventTimestamp = event.content?.ts || event.origin_server_ts || Date.now()
        const kinesisEvent: any = {
            writeKey: "spaces",
            userId,
            timestamp: new Date(eventTimestamp).toISOString(),
            properties: {
                businessId,
                communityId: community?.id,
                spaceId,
                length: event.content?.body?.length,
                event
            },
        }

        if (event.type === "m.room.member" && event.content?.membership === "join")
            kinesisEvent.event = "spaceJoin"
        else if (event.type === "v.room.enter")
            kinesisEvent.event = "spaceEnter"
        else if (event.type === "m.room.member" && event.content?.membership === "leave")
            kinesisEvent.event = "spaceLeave"
        else if (event.type === "m.room.redaction") {
            kinesisEvent.event = "messageDelete"
            // const originalEvent = await services.matrix.getEvent(accessToken, event.room_id, event.redacts)
            // kinesisEvent.properties.relatesTo = event.redacts
            // kinesisEvent.properties.messageType = originalEvent.type
        } else {
            kinesisEvent.event = "messagePost"
        //     kinesisEvent.properties.messageType = event.type
        //     kinesisEvent.properties.content = event.content
        //     kinesisEvent.properties.relatesTo = event.content["m.relates_to"]?.event_id
        //     kinesisEvent.properties.length = event.content?.body?.length
        } 

        // Send kinesis analytics event
        if (kinesisEvent.event) {
            utils.logger.info("Sending event to firehose:", JSON.stringify(kinesisEvent));
            await putFirehoseEvent(kinesisEvent);
        }

    } catch (err) {
        utils.logger.error("Error sending event to firehose:", err, event.type, JSON.stringify(event));
    }
}


export default async function (lambdaEvent: http.lambda.LambdaEvent) {
    utils.logger.info("Matrix events:", lambdaEvent.pathParameters.txnId, JSON.stringify(lambdaEvent.body));
    const pg = await services.pg.connectForEvent(lambdaEvent);

    for (const event of lambdaEvent.body.events) {

        if (!event.user_id) continue
        if (!event.room_id) continue

        const userId = event.user_id.split(":")[0].split("@")[1]
        utils.logger.info("Matrix event userId:", userId);

        if (userId === "vatom") continue

        const utf8 = new TextEncoder();
        const bodyStr = JSON.stringify(event);
        const sigAlg = { name: "HMAC", hash: "SHA-256" };

        try {
            // Determine the business ID from the event
            const ownerMatrixToken = await services.matrix.getMatrixToken(userId);
            let aliases = []
            try {
                aliases = await services.matrix.getRoomAliases(ownerMatrixToken, event.room_id);
            } catch (e) {
                utils.logger.warn("Room aliases could not be found for ", event.room_id, e);
                continue
            }

            utils.logger.info("Matrix room aliases:", aliases);
            const businessId = aliases.find(a => a.split(":")[0].split(".").length === 2)?.split(":")[0].split(".")[0].split('#')[1]

            utils.logger.info("BusinessId:", businessId);

            if (!businessId) continue

            const plugins = await SparkPlugins.list(pg, businessId)

            await Promise.all(plugins.map(async plugin => {
                try {
                    const sigKey = await crypto.subtle.importKey(
                        "raw",
                        utf8.encode(plugin.commSecret),
                        sigAlg,
                        false, // not exportable
                        ["sign"],
                    );

                    const sigBuf = await crypto.subtle.sign(sigAlg, sigKey, utf8.encode(bodyStr));
                    const sig = Buffer.from(sigBuf).toString("base64");

                    utils.logger.info("Sending event to plugin:", plugin.id, bodyStr)

                    await axios.post(plugin.commUrl + "/events", bodyStr, {
                        headers: {
                            "content-type": "application/json; charset=utf-8",
                            "x-signature-sha256": sig,
                        },
                    });
                } catch (e) {
                    utils.logger.error("Error sending event to plugin:", plugin.id, e);
                }
            }));

            // Verify that the communities are initialized
            await Communities.sync(pg, ownerMatrixToken, businessId);
            
            await sendEventToFirehose(ownerMatrixToken, businessId, userId, event);
        } catch (e) {

            // If the user is not found, just ignore and continue     
            if (e instanceof services.user.UserNotFoundError) continue

            utils.logger.error("Error processing matrix event:", e);
        }
        
    }

    return http.responses.ok({});
}
