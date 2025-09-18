// IMPORTANT: CRITICAL PERFORMANCE CHANGE
// DUE TO ISSUES UPDATING VARIOUS/IO LATEST FRAMEWORK THIS IS PLACED. 
// PLEASE REMOVE ONCE THE FRAMEWORK IS UPDATED
import NodeCache = require("node-cache");
import axios from "axios";

const { MATRIX_SERVER_URL } = process.env

type LoginResponse = {
    user_id: string,
    access_token: string,
    home_server: string,
    device_id?: string,
    well_known: {
        "m.homeserver": {
            base_url: string
        }
    }
}

const ax = axios.create({
    baseURL: MATRIX_SERVER_URL,
});


export const memo = async <T>(cache: NodeCache, key: string, fn: () => Promise<T>) => {
	const fromCache: T | undefined = cache.get(key);
	if (fromCache !== undefined) return fromCache; // Must test against undefined in case the value is a boolean

	const data = await fn();
	cache.set(key, data);
	return data;
};

const tokenCache = new NodeCache({ stdTTL: 5 * 60 /* 5 mins */, checkperiod: 20 });
export async function exchangeTokenWithDevice(accessToken: string, deviceId = "backend"): Promise<LoginResponse> {

    return await memo(tokenCache, accessToken, async () => {
        const { data } = await ax.post<any>(`/_matrix/client/v3/login`, {
            type: "org.matrix.login.jwt",
            token: accessToken,
            device_id: deviceId
        })
        return data;
    });
}