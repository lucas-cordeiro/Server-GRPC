package br.com.lucascordeiro.klever

import br.com.lucascordeiro.klever.*
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.firestore.FieldValue
import com.google.cloud.firestore.Firestore
import com.google.cloud.firestore.ListenerRegistration
import com.google.firebase.FirebaseApp
import com.google.firebase.FirebaseOptions
import com.google.firebase.cloud.FirestoreClient
import io.grpc.Server
import io.grpc.ServerBuilder
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.channels.sendBlocking
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.callbackFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.runBlocking
import java.io.FileInputStream
import kotlin.coroutines.CoroutineContext


class KleverServer(val port: Int, val firestore: Firestore): CoroutineScope {

    override val coroutineContext = Dispatchers.IO

    val server: Server = ServerBuilder
        .forPort(port)
        .addService(KleverService(firestore, this))
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
        coroutineContext.cancel()
        server.shutdown()
    }

    fun blockUntilShutdown() {
        server.awaitTermination()
    }

    private class KleverService(val firestore: Firestore, scope: CoroutineScope) : KleverServiceGrpcKt.KleverServiceCoroutineImplBase() {

        override fun getBankAccount(request: GetBankAccountRequest): Flow<BankAccount> = callbackFlow {
            val bankAccountId = request.bankAccountId.toString()
            println("getBankAccount: $bankAccountId")

            val bankAccountRef = firestore.collection("bankaccounts").document(bankAccountId)

            val listenerRegistration = bankAccountRef.addSnapshotListener { snapshot, error ->
                if (error != null)
                    throw error

                if (snapshot != null && snapshot.exists()) {
                    val bankAccount = BankAccount.newBuilder()
                        .setId(snapshot.id)
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

        override fun getBankAccountCoins(request: GetBankAccountCoinsRequest): Flow<ListOfBankAccountCoin> = callbackFlow {
            val bankAccountId = request.bankAccountId.toString()
            println("getBankAccountCoins: $bankAccountId")

            val bankAccountCoinsResponse: MutableList<BankAccountCoin> = ArrayList()

            val coinsRef =  firestore.collection("coins").whereArrayContains("bankAccountsId", request.bankAccountId)
            val coinsSnapshot = coinsRef.get().get()

            if (coinsSnapshot != null && coinsSnapshot.documents.isNotEmpty()) {
                val coins = coinsSnapshot.documents.map {
                    Coin.newBuilder()
                        .setId(it.id)
                        .setName(it.getString("name"))
                        .setShortName(it.getString("shortName"))
                        .setPrice(it.getDouble("price")?:0.0)
                        .setPercent(it.getDouble("percent")?.toFloat()?:0f)
                        .setIconUrl(it.getString("iconUrl"))
                        .build()
                }

                println("Coins: ${coins.size}")

                coins.forEach { coin ->
                    val index = bankAccountCoinsResponse.indexOfFirst { it.coinId == coin.id }
                    if(index >= 0){
                        val bankAccountCoin = bankAccountCoinsResponse[index]
                        bankAccountCoinsResponse[index] = BankAccountCoin.newBuilder()
                            .setId(bankAccountCoin.id)
                            .setCoin(coin)
                            .setCoinId(coin.id)
                            .setAmount(bankAccountCoin.amount)
                            .build()

                    }else{
                        bankAccountCoinsResponse.add(
                            BankAccountCoin.newBuilder()
                                .setCoin(coin)
                                .setCoinId(coin.id)
                                .build()
                        )
                    }
                }

                sendBlocking(
                    ListOfBankAccountCoin.newBuilder()
                        .setCount(bankAccountCoinsResponse.size.toLong())
                        .addAllData(bankAccountCoinsResponse)
                        .build()
                )
            }

            val bankAccountCoinsRef = firestore.collection("bankaccounts").document(bankAccountId).collection("coins")
            val bankAccountCoinsListener = bankAccountCoinsRef.addSnapshotListener { coinsSnapshot, error ->
                if (error != null)
                    throw error

                if (coinsSnapshot != null && coinsSnapshot.documents.isNotEmpty()) {
                    val bankAccountCoins = coinsSnapshot.documents.map {
                        BankAccountCoin.newBuilder()
                            .setId(it.id)
                            .setCoinId(it.getString("coinId"))
                            .setAmount(it.getDouble("amount")?:0.0)
                            .build()
                    }

                    bankAccountCoins.forEach { bankAccountCoin ->
                        val index = bankAccountCoinsResponse.indexOfFirst { it.coinId == bankAccountCoin.coinId }
                        if(index >= 0){
                            val bankAccountCoinTemp = bankAccountCoinsResponse[index]
                            bankAccountCoinsResponse[index] = BankAccountCoin.newBuilder()
                                .setId(bankAccountCoin.id)
                                .setCoin(bankAccountCoinTemp.coin)
                                .setCoinId(bankAccountCoin.coinId)
                                .setAmount(bankAccountCoin.amount)
                                .build()
                        }else{
                            bankAccountCoinsResponse.add(
                                BankAccountCoin.newBuilder()
                                    .setId(bankAccountCoin.id)
                                    .setAmount(bankAccountCoin.amount)
                                    .setCoinId(bankAccountCoin.id)
                                    .build()
                            )
                        }
                    }

                    sendBlocking(
                        ListOfBankAccountCoin.newBuilder()
                            .setCount(bankAccountCoinsResponse.size.toLong())
                            .addAllData(bankAccountCoinsResponse)
                            .build()
                    )
                }
            }

            awaitClose {
                bankAccountCoinsListener.remove()
            }
        }

        override fun getBankAccountTransactions(request: GetBankAccountTransactionsRequest): Flow<ListOfBankAccountTransaction> = callbackFlow {
            val bankAccountId = request.bankAccountId
            println("getBankAccountTransactions: $bankAccountId")

            val transactionsRef = firestore.collection("transactions").whereEqualTo("bankAccountId", bankAccountId)
            val listenerRegistration = transactionsRef.addSnapshotListener { snapshot, error ->
                if (error != null)
                    throw error

                if (snapshot != null && snapshot.documents.isNotEmpty()) {
                    val transactions = snapshot.documents.map {
                        BankAccountTransaction.newBuilder()
                            .setId(it.id)
                            .setAmount(it.getDouble("amount")?:0.0)
                            .setBankAccountId(bankAccountId)
                            .setTransferDate(it.getLong("transferDate")?:0L)
                            .setCredit(it.getBoolean("credit")?:false)
                            .build()
                    }

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

        override fun getBankAccountCoinsTransactions(request: GetBankAccountCoinsTransactionsRequest): Flow<ListOfBankAccountCoinTransaction> =  callbackFlow {
            val bankAccountId = request.bankAccountId
            val coinId = request.coinId
            println("getBankAccountTransactions: $bankAccountId")

            val transactionsRef = firestore.collection("bankaccounts").document(bankAccountId).collection("coins").document(coinId).collection("transactions")
            val listenerRegistration = transactionsRef.addSnapshotListener { snapshot, error ->
                if (error != null)
                    throw error

                if (snapshot != null && snapshot.documents.isNotEmpty()) {
                    val transactions = snapshot.documents.map {
                        BankAccountCoinTransaction.newBuilder()
                            .setId(it.id)
                            .setAmount(it.getDouble("amount")?:0.0)
                            .setCredit(it.getBoolean("credit")?:false)
                            .setTransferDate(it.getLong("transferDate")?:0L)
                            .build()
                    }

                    sendBlocking(
                        ListOfBankAccountCoinTransaction
                        .newBuilder()
                        .setCount(transactions.size.toLong())
                        .addAllData(transactions)
                        .build()
                    )
                }else{
                    sendBlocking(ListOfBankAccountCoinTransaction
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

        override suspend fun addBankAccountTransaction(request: BankAccountTransaction): BankAccountTransaction {
            println("addBankAccountTransaction")

            val amount = request.amount * if(request.credit) 1 else -1
            val requestHash = HashMap<String, Any?>()
            requestHash["bankAccountId"] = request.bankAccountId
            requestHash["credit"] = request.credit
            requestHash["amount"] = amount
            requestHash["transferDate"] = System.currentTimeMillis() / 1000

            val documentRef = firestore.collection("transactions").add(requestHash).get()

            firestore.collection("bankaccounts").document(request.bankAccountId).update("balance",  FieldValue.increment(amount)).get()

            return BankAccountTransaction
                .newBuilder(request)
                .setId( documentRef.id)
                .setTransferDate(System.currentTimeMillis() / 1000)
                .build()
        }

        override suspend fun addBankAccountCoinTransaction(request: BankAccountCoinTransaction): BankAccountCoinTransaction {
            println("addBankAccountCoinTransaction")
            val amount = request.amount * if(request.credit) 1 else -1

            val requestHash = HashMap<String, Any?>()
            requestHash["credit"] = request.credit
            requestHash["amount"] = amount
            requestHash["transferDate"] = System.currentTimeMillis() / 1000

            val bankAccountCoinSnapshotOld = firestore.collection("bankaccounts").document(request.bankAccountId).collection("coins").document(request.coinId).get().get()
            val bankAccountCoinOld =  BankAccountCoin.newBuilder()
                .setAmount(bankAccountCoinSnapshotOld.getDouble("amount")?:0.0)
                .setCoinId(bankAccountCoinSnapshotOld.getString("coinId"))
                .build()

            firestore.collection("bankaccounts").document(request.bankAccountId).collection("coins").document(request.coinId).update("amount",  FieldValue.increment(amount))
            val bankAccountCoinSnapshot = firestore.collection("bankaccounts").document(request.bankAccountId).collection("coins").document(request.coinId).get().get()

            val bankAccountCoin =  BankAccountCoin.newBuilder()
                .setAmount(bankAccountCoinSnapshot.getDouble("amount")?:0.0)
                .setCoinId(bankAccountCoinSnapshot.getString("coinId"))
                .build()

            val coinSnapshot = firestore.collection("coins").document(bankAccountCoin.coinId).get().get()
            val coin = Coin.newBuilder()
                .setId(coinSnapshot.id)
                .setName(coinSnapshot.getString("name"))
                .setShortName(coinSnapshot.getString("shortName"))
                .setPrice(coinSnapshot.getDouble("price")?:0.0)
                .setPercent(coinSnapshot.getDouble("percent")?.toFloat()?:0f)
                .setIconUrl(coinSnapshot.getString("iconUrl"))
                .build()

            val balance = amount * coin.price
            firestore.collection("bankaccounts").document(request.bankAccountId).update("balance",  FieldValue.increment(balance)).get()

            val documentRef = firestore
                .collection("bankaccounts")
                .document(request.bankAccountId)
                .collection("coins")
                .document(request.coinId)
                .collection("transactions")
                .add(requestHash)
                .get()

            return BankAccountCoinTransaction
                .newBuilder(request)
                .setId( documentRef.id)
                .setTransferDate(System.currentTimeMillis() / 1000)
                .build()
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