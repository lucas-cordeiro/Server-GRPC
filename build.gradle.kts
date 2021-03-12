import com.google.protobuf.gradle.generateProtoTasks
import com.google.protobuf.gradle.id
import com.google.protobuf.gradle.ofSourceSet
import com.google.protobuf.gradle.plugins
import com.google.protobuf.gradle.protobuf
import com.google.protobuf.gradle.protoc
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    application
    id("org.springframework.boot") version "2.4.0"
    id("io.spring.dependency-management") version "1.0.10.RELEASE"
    id("com.google.protobuf") version "0.8.13"
    kotlin("jvm") version "1.4.31"
    kotlin("plugin.spring") version "1.4.31"
}

allprojects {
    repositories {
        mavenLocal()
        mavenCentral()
        jcenter()
        google()
    }
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    implementation("io.grpc:grpc-protobuf:1.33.1")
    implementation("io.grpc:grpc-stub:1.33.1")
    implementation("io.grpc:grpc-netty:1.33.1")
    compileOnly("javax.annotation:javax.annotation-api:1.3.2")
    api("com.google.protobuf:protobuf-java-util:3.13.0")
    implementation("io.grpc:grpc-all:1.33.1")
    api("io.grpc:grpc-kotlin-stub:0.2.1")
    implementation("io.grpc:protoc-gen-grpc-kotlin:0.1.5")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.3.9")
    implementation("com.google.protobuf:protobuf-gradle-plugin:0.8.13")
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
    testImplementation("org.springframework.boot:spring-boot-starter-test")
}

group = "br.com.lucascordeiro.klever"
version = "0.0.1-SNAPSHOT"
java.sourceCompatibility = JavaVersion.VERSION_11

tasks.withType<Test> {
    useJUnitPlatform()
}

tasks.withType<KotlinCompile> {
    kotlinOptions {
        freeCompilerArgs = listOf("-Xjsr305=strict")
        jvmTarget = "11"
    }
}

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
}

application {
    mainClassName = "br.com.lucascordeiro.klever.generated.main.grpckt.br.com.lucascordeiro.klever.BookServerKt"
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:3.10.1"
    }
    generatedFilesBaseDir = "$projectDir/src/main/kotlin/br.com.lucascordeiro.klever/generated"
    plugins {
        id("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:1.33.1"
        }
        id("grpckt") {
            artifact = "io.grpc:protoc-gen-grpc-kotlin:0.1.5"
        }
    }
    generateProtoTasks {
        all().forEach {
            it.plugins {
                id("grpc")
                id("grpckt")
            }
        }
    }
}