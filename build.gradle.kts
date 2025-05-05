plugins {
    id("java")
    id("org.springframework.boot") version "3.4.0" // Adjust version as needed
    id("io.spring.dependency-management") version "1.1.6"
    kotlin("jvm") version "1.9.10"
    kotlin("plugin.spring") version "1.9.10"

}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

ext {
    set("springCloudVersion", "2024.0.0")
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter")
    implementation("org.springframework.boot:spring-boot-starter-json")
    implementation("org.springframework.cloud:spring-cloud-starter-stream-kafka:4.0.5")
    implementation("org.springframework.cloud:spring-cloud-stream-binder-kafka")
    implementation("com.solace.spring.cloud:spring-cloud-stream-binder-solace:5.7.0")
    implementation("com.google.code.gson:gson:2.13.0")
    implementation("org.mongodb:mongodb-driver-sync:4.11.1")
    implementation("org.mongodb:mongodb-driver-core:4.11.1")

    implementation("org.jetbrains.kotlin:kotlin-reflect")
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")

    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}
dependencyManagement {
    imports {
        mavenBom("org.springframework.cloud:spring-cloud-dependencies:2024.0.0")
    }
}

tasks.test {
    useJUnitPlatform()
}