plugins {
    java
}

// The Flow release version; a test release is published as X.Y.Z-SNAPSHOT.
val flowVersion = providers.gradleProperty("flowVersion").getOrElse("0.2.1-SNAPSHOT")

repositories {
    mavenCentral()
    // Flow test releases (X.Y.Z-SNAPSHOT) live in the Sonatype snapshot repository.
    maven {
        url = uri("https://central.sonatype.com/repository/maven-snapshots/")
        mavenContent { snapshotsOnly() }
    }
}

tasks.withType<JavaCompile>().configureEach {
    options.release.set(17)
}

dependencies {
    implementation("tech.ytsaurus:flow-runner:$flowVersion")
    // @Entity marks the per-key state POJO for the SDK's YSON serializer.
    implementation("javax.persistence:persistence-api:1.0")
    runtimeOnly("org.apache.logging.log4j:log4j-slf4j2-impl:2.25.1")

    testImplementation("tech.ytsaurus:flow-core:$flowVersion")
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.2")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher:1.10.2")
}

tasks.named<Test>("test") {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
    }
}

// Collects the pipeline jar and its runtime deps into lib/ — the classpath both the launch
// command (`-cp 'lib/*'`) and the Flow runner (it ships every jar on its own classpath into the
// companion job) use. lib/ is gitignored; rebuild it with `gradle installLib`.
tasks.register<Sync>("installLib") {
    dependsOn(tasks.jar)
    from(tasks.jar)
    from(configurations.runtimeClasspath)
    into(layout.projectDirectory.dir("lib"))
}
