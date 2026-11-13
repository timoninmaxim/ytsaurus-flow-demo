plugins {
    java
    application
}

repositories { mavenCentral() }
java { toolchain { languageVersion.set(JavaLanguageVersion.of(17)) } }

dependencies {
    implementation("javax.persistence:persistence-api:1.0")
    implementation("tech.ytsaurus:flow-spring-boot-starter:0.3.0")
    testImplementation("tech.ytsaurus:flow-test-utils:0.3.0")
    testImplementation("org.junit.jupiter:junit-jupiter:6.0.2")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher:6.0.2")
    testImplementation("org.springframework.boot:spring-boot-test:4.0.2")
    testImplementation("org.springframework.boot:spring-boot-test-autoconfigure:4.0.2")
}

application {
    mainClass.set("tech.ytsaurus.flow.examples.wordcount.WordCountApplication")
}

tasks.test {
    useJUnitPlatform()
    testLogging { events("passed", "skipped", "failed") }
}
