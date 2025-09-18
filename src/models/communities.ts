import { services } from "@varius.io/framework";
import MatrixDB from "#src/services/mxdb";
import { exchangeTokenWithDevice } from "#src/services/matrix";

export interface Community {
	id: string; // This is the matrix room id
	businessId: string;
	name: string;
	alias: string;
	isDefault: boolean;
	description?: string;
	icon?: string;
	isArchived?: boolean;
	// matrixRoomId: string; 
}

const { MATRIX_DOMAIN } = process.env

function rowToCommunity(row: any): Community {

	if (row === undefined) return undefined

	return {
		id: row.id,
		businessId: row.business_id,
		name: row.display_name,
		alias: row.alias,
		isDefault: row.default,
		description: row.description,
		icon: row.icon,
		isArchived: row.archived
	}
}

export async function list(
	pg: services.pg,
	businessId: string
): Promise<Community[]> {
	const { rows } = await pg.select(
		"community",
		{
			business_id: businessId,
			archived: false
		}
	);

	return rows.map(rowToCommunity);
}

export async function get(
	pg: services.pg,
	id: string
): Promise<Community> {
	const { rows } = await pg.select(
		"community",
		{
			id: id
		}
	);

	return rowToCommunity(rows[0]);

}

export async function getByChildId(
	id: string
): Promise<Community> {
	const parentRoomEvent = await MatrixDB.withConnection<any>(async (matrixdb) => {
		// Get the community for the space
		const { rows } = await matrixdb.select("current_state_events", { 
			type: 'm.space.child',
			state_key: id
		})

		return rows[0]
	})

	if (!parentRoomEvent) return undefined

	const community = await services.pg.withConnection<Community>(async (pg) => {
		return await get(pg, parentRoomEvent.room_id)
	});

	return community

}

export async function sync(pg: services.pg, matrixOrVatomToken: string, business: services.businesses.Business) {
	const communities = await list(pg, business.id)
	
	// The list of communities should be at least one (the default one)
	if (communities.length === 0 && business.matrixSpaceId) {

		let token = matrixOrVatomToken
		if (matrixOrVatomToken.length > 100) {
			// This is a vatom token, exchange it for a matrix token
			const { access_token } = await exchangeTokenWithDevice(matrixOrVatomToken)
			token = access_token
		}
		
		// List the spaces where the parent is the 
		const response = await services.matrix.listChildren(token, business.matrixSpaceId);
		const matrixCommunities = response.rooms.filter(r => r.room_type === "m.space").map(r => {
			return {
				id: r.room_id,
				businessId: business.id,
				name: r.name,
				isArchived: false,
				icon: r.avatar_url,
				isDefault: r.canonical_alias === `#${business.id}:${MATRIX_DOMAIN}`,
				alias: r.canonical_alias
			}
		})

		for (const community of matrixCommunities) {
			await create(pg, community, business.ownerId)
		}
		return matrixCommunities;
	} 
	return communities
}

export async function create(
	pg: services.pg,
	community: Community,
	userId: string,
): Promise<void> {

	await pg.insert(
		"community",
		{
			id: community.id,
			business_id: community.businessId,
			display_name: community.name,
			alias: community.alias,
			default: community.isDefault,
			description: community.description,
			icon: community.icon,
			created_at: new Date(),
			created_by_id: userId,
		}
	);
}
