import io.appwrite.Client
import io.appwrite.extensions.JsonExtensions.fromJson
import io.appwrite.services.Database

class FilteredResponse(
    val documents: Collection<Any>
)

val client by lazy {
    Client()
        .setEndpoint(System.getenv("APPWRITE_ENDPOINT"))
        .setProject(System.getenv("APPWRITE_PROJECT_ID"))
        .setKey(System.getenv("APPWRITE_API_KEY"))
}

val db by lazy {
    Database(client)
}

suspend fun main(args: Array<String>) {
    println("Cleaning up empty rooms...")
    deleteEmptyRooms(50, 0)
    println("Complete!")
}

private tailrec suspend fun deleteEmptyRooms(pageSize: Int, offset: Int) {
    val roomCollectionId = System.getenv("APPWRITE_ROOM_COLLECTION_ID")

    val docsResponse = db.listDocuments(
        roomCollectionId,
        listOf("participants=0"),
        pageSize,
        offset
    )

    val docs = docsResponse.body
        ?.string()
        ?.fromJson(FilteredResponse::class.java)
        ?.documents
        ?: return

    for (doc in docs) {
        val room = doc as Map<String, *>
        val roomId = room["\$id"].toString()

        println("Deleting room $roomId...")

        deleteRoomPaths(roomId, pageSize, 0)

        db.deleteDocument(
            roomCollectionId,
            roomId
        )
    }

    if (docs.size >= pageSize) {
        deleteEmptyRooms(pageSize, offset + pageSize)
    }
}

private tailrec suspend fun deleteRoomPaths(roomId: String, pageSize: Int, offset: Int) {
    val pathCollectionId = System.getenv("APPWRITE_PATH_COLLECTION_ID")

    val roomPaths = db.listDocuments(
        pathCollectionId,
        listOf("roomId=$roomId"),
        pageSize,
        offset
    )

    val docs = roomPaths.body
        ?.string()
        ?.fromJson(FilteredResponse::class.java)
        ?.documents
        ?: return

    println("Deleting paths for room $roomId...")

    for (doc in docs) {
        val path = doc as Map<String, *>
        val pathId = path["\$id"].toString()

        db.deleteDocument(
            pathCollectionId,
            pathId
        )
    }

    if (docs.size >= pageSize) {
        deleteRoomPaths(roomId, pageSize, offset + pageSize)
    }
}
