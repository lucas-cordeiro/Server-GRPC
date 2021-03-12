package br.com.lucascordeiro.klever.generated.main.grpckt.br.com.lucascordeiro.klever

import br.com.lucascordeiro.klever.Book
import br.com.lucascordeiro.klever.BookServiceGrpcKt
import br.com.lucascordeiro.klever.GetBookRequest
import io.grpc.Server
import io.grpc.ServerBuilder
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow

class BookServer(val port: Int) {
    val server: Server = ServerBuilder
        .forPort(port)
        .addService(BookService())
        .build()

    fun start() {
        server.start()
        println("Server started, listening on $port")
        Runtime.getRuntime().addShutdownHook(
            Thread {
                println("*** shutting down gRPC server since JVM is shutting down")
                stop()
                println("*** server shut down")
            }
        )
    }

    private fun stop() {
        server.shutdown()
    }

    fun blockUntilShutdown() {
        server.awaitTermination()
    }

    private class BookService : BookServiceGrpcKt.BookServiceCoroutineImplBase() {
        override suspend fun getBook(request: GetBookRequest) : Book {
            return Book
                .newBuilder()
                .setTitle("Pequeno Principe")
                .setAuthor("Desconhecido")
                .setIsbn(1)
                .build()
        }

        override fun getBookStream(request: GetBookRequest): Flow<Book> = flow {
            while (true) {
                delay(1000)
                emit(Book
                    .newBuilder()
                    .setTitle("Pequeno Principe")
                    .setAuthor("Desconhecido")
                    .setIsbn(1)
                    .build())
            }
        }
    }
}

fun main() {
    val port = System.getenv("PORT")?.toInt() ?: 50052
    val server = BookServer(port)
    server.start()
    server.blockUntilShutdown()
}