plugins {
    java
    checkstyle
    jacoco
    id("com.diffplug.spotless") version "7.0.2"
}

group = "com.lsmtreestore"
version = "0.1.0-SNAPSHOT"

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

repositories {
    mavenCentral()
}

dependencies {
    // Logging — SLF4J facade with Logback implementation
    implementation("org.slf4j:slf4j-api:2.0.17")
    runtimeOnly("ch.qos.logback:logback-classic:1.5.32")

    // Testing — JUnit 5 + AssertJ (fluent assertions) + Mockito
    testImplementation(platform("org.junit:junit-bom:5.11.3"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.assertj:assertj-core:3.26.3")
    testImplementation("org.mockito:mockito-core:5.14.2")
    testImplementation("org.mockito:mockito-junit-jupiter:5.14.2")
}

// ---------------------------------------------------------------------------
// Testing
// ---------------------------------------------------------------------------
tasks.test {
    useJUnitPlatform()
    // Show test results in console for CI visibility
    testLogging {
        events("passed", "skipped", "failed")
    }
}

// ---------------------------------------------------------------------------
// Code Style — Google Java Format via Spotless
// ---------------------------------------------------------------------------
spotless {
    java {
        googleJavaFormat("1.24.0")
        removeUnusedImports()
        trimTrailingWhitespace()
        endWithNewline()
    }
}

// ---------------------------------------------------------------------------
// Checkstyle — Google Java Style checks
// ---------------------------------------------------------------------------
checkstyle {
    toolVersion = "10.20.1"
    configFile = file("config/checkstyle/google_checks.xml")
    maxWarnings = 0
    // Don't fail on empty source sets during initial setup
    isIgnoreFailures = false
}

// ---------------------------------------------------------------------------
// Code Coverage — JaCoCo
// ---------------------------------------------------------------------------
jacoco {
    toolVersion = "0.8.12"
}

tasks.jacocoTestReport {
    dependsOn(tasks.test)
    reports {
        xml.required.set(true)
        html.required.set(true)
    }
}
