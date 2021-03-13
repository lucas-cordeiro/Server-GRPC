package br.com.lucascordeiro.klever

import br.com.lucascordeiro.klever.*
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.firestore.Firestore
import com.google.firebase.FirebaseApp
import com.google.firebase.FirebaseOptions
import com.google.firebase.cloud.FirestoreClient
import io.grpc.Server
import io.grpc.ServerBuilder
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.channels.sendBlocking
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.callbackFlow
import java.io.FileInputStream


class KleverServer(val port: Int, val firestore: Firestore) {
    val server: Server = ServerBuilder
        .forPort(port)
        .addService(KleverService(firestore))
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

    private class KleverService(val firestore: Firestore) : KleverServiceGrpcKt.KleverServiceCoroutineImplBase() {
        override fun getBankAccount(request: GetBankAccountRequest): Flow<BankAccount> = callbackFlow {
            val bankAccountId = request.bankAccountId.toString()
            println("GetBankAccount: $bankAccountId")

            val bankAccountRef = firestore.collection("bankaccounts").document(bankAccountId)

            val listenerRegistration = bankAccountRef.addSnapshotListener { snapshot, error ->
                if (error != null)
                    throw error

                if (snapshot != null && snapshot.exists()) {
                    println("exist")
                    val bankAccount = BankAccount.newBuilder()
                        .setId(snapshot.id.toLong())
                        .setBalance(snapshot.getDouble("balance")?:0.0)
                        .setName(snapshot.getString("name"))
                        .setProfilePicUrl(snapshot.getString("profilePicUrl"))
                        .build()
                    sendBlocking(bankAccount)
                } else {
                    throw NullPointerException("BankAccount not found")
                }
            }

            awaitClose {
                println("close")
                listenerRegistration.remove()
            }
        }

        override fun getBankAccountTransactions(request: GetBankAccountTransactionsRequest): Flow<ListOfBankAccountTransaction> = callbackFlow {
            val bankAccountId = request.bankAccountId.toString()
            println("getBankAccountTransactions: $bankAccountId")

            val transactionsRef = firestore.collection("transactions").whereEqualTo("bankAccountId", bankAccountId.toLong())
            val listenerRegistration = transactionsRef.addSnapshotListener { snapshot, error ->
                if (error != null)
                    throw error

                if (snapshot != null && snapshot.documents.isNotEmpty()) {
                    val transactions = snapshot.documents.map {
                        BankAccountTransaction.newBuilder()
                            .setId(it.id.toLong())
                            .setAmount(it.getDouble("amount")?:0.0)
                            .setBankAccountId(bankAccountId.toLong())
                            .setTransferDate(it.getLong("transferDate")?:0L)
                            .build()
                    }
                    println("transactions ${transactions.size}")
                    sendBlocking(ListOfBankAccountTransaction
                        .newBuilder()
                        .setCount(transactions.size.toLong())
                        .addAllData(transactions)
                        .build()
                    )
                }else{
                    sendBlocking(ListOfBankAccountTransaction
                        .newBuilder()
                        .setCount(0L)
                        .addAllData(emptyList())
                        .build()
                    )
                }
            }

            awaitClose {
                println("close")
                listenerRegistration.remove()
            }
        }
    }
}

fun main() {
//    val serviceAccount = FileInputStream("C:/Users/ITX/Documents/Klever/klever-coin-firebase-adminsdk-5yvr5-fb477f4036.json")

    val options = FirebaseOptions.builder()
//        .setCredentials(GoogleCredentials.fromStream(serviceAccount))
        .setCredentials(GoogleCredentials.getApplicationDefault())
        .setDatabaseUrl("https://klever-coin.firebaseio.com/")
        .setProjectId("klever-coin")
        .build()

    FirebaseApp.initializeApp(options)

    val port = System.getenv("PORT")?.toInt() ?: 50052
    val server = KleverServer(
        port = port,
        firestore =  FirestoreClient.getFirestore(),
    )
    server.start()
    server.blockUntilShutdown()
}