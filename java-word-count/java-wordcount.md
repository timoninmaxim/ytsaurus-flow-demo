# Java WordCount: Gradle и vanilla

Последовательность запуска Java-примера `word_count` с опубликованным SDK и образом
Flow Java. Controller и Worker — в YT vanilla operation.
Exec nodes должны использовать backend `cri` с containerd и иметь доступ к GHCR.

Это отдельный Gradle-проект, использующий исходники
[примера](.).

В командах и `pipeline.yson` заменить `YOUR_CLUSTER_HTTP_PROXY`, `YOUR_USER` и
`YOUR_POOL` своими значениями. Все команды ниже выполнять в одной shell-сессии:

```bash
export RUN_ID="java-wordcount-$(date +%Y%m%d-%H%M%S)"
export TASK="$HOME/flow-runs/$RUN_ID"
export YT_PROXY=YOUR_CLUSTER_HTTP_PROXY
export YT_FLOW_ROOT=//home/YOUR_USER/java-word-count
export YT_POOL=YOUR_POOL
mkdir -p "$TASK/source" "$TASK/app" "$TASK/gradle-cache"
cd "$TASK"
```

`RUN_ID` задаёт имя локального запуска и Docker-контейнера раннера, `TASK` —
рабочий каталог с исходниками, Gradle-проектом, venv, кешем и логами.
`YT_PROXY` — HTTP proxy кластера, `YT_FLOW_ROOT` — каталог объектов pipeline в YT,
`YT_POOL` — scheduler pool. Значения YT-переменных соответствуют готовому конфигу
из шага 4.

## 1. Собрать Gradle-проект

Создать `app/settings.gradle.kts`:

```kotlin
rootProject.name = "wordcount"
```

Создать `app/build.gradle.kts`:

```kotlin
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
```

Выполнить сборку с тестом:

```bash
docker run --rm --network host --user "$(id -u):$(id -g)" \
    -e GRADLE_USER_HOME=/gradle-cache \
    -v "$TASK/gradle-cache:/gradle-cache" -v "$TASK/app:/app" -w /app \
    gradle:8.14.3-jdk17 \
    gradle --no-daemon --console=plain build installDist \
    > "$TASK/gradle-build.log" 2>&1
```

## 2. Подготовить Python venv и аутентификацию

Создать окружение Python 3.12 и установить зависимости из публичного PyPI:

```bash
python3.12 -m venv "$TASK/.venv"
source "$TASK/.venv/bin/activate"
python3 -m pip install --index-url https://pypi.org/simple \
    ytsaurus-client==0.13.55 \
    ytsaurus-yson==0.4.10 \
    ytsaurus-flow-yt-sync-mini==0.3.0

export YT_TOKEN="$(cat "$HOME/.yt/token")"
yt exists "$YT_FLOW_ROOT"
```

При следующих запусках активировать готовое окружение командой
`source "$TASK/.venv/bin/activate"` и повторно экспортировать `YT_TOKEN`.
Все команды `yt` и `python3` ниже выполняются в нём.

Для нового запуска `yt exists` должен вернуть `false`. Docker передаёт
`YT_TOKEN` из окружения Java-раннеру через `-e YT_TOKEN`, затем токен
доставляется jobs через secure vault vanilla operation.
В конфиги и образ токен не записывается. Эти команды не следует выполнять с `set -x`.

## 3. Создать очередь, consumer и pipeline через yt_sync_mini

Сохранить [скрипт из example](yt_sync.py) в
`$TASK/source/yt_sync.py`. Перед bootstrap согласовать `YT_PROXY`, `YT_POOL` и
`YT_FLOW_ROOT` с [готовым конфигом](pipeline.yson)
из шага 4. Выполнить в активированном venv:

```bash
python3 "$TASK/source/yt_sync.py" > "$TASK/bootstrap.log" 2>&1
```

Скрипт берёт HTTP proxy из `YT_PROXY`, а директорию запуска — из `YT_FLOW_ROOT`.
Параметры `--cluster`, `--folder` и `--queue-tablet-count` позволяют переопределить
эти значения и число tablets входной очереди (по умолчанию один).

Получатся `input_queue`, `consumer`, `producer`, `pipeline` и системные таблицы
Flow. Bundle и medium выбираются по умолчанию кластера. Повторный запуск
сохраняет существующие объекты.

## 4. Подготовить runner config

Сохранить [готовый конфиг](pipeline.yson) в
`$TASK/source/pipeline.yson` и скопировать его в рабочий каталог приложения:

```bash
cp "$TASK/source/pipeline.yson" "$TASK/app/pipeline.yson"
```

При выборе другого кластера, пула или каталога отредактировать `cluster_url`, `vanilla.pool`, `path`, `vanilla.cache_path`, `queue_path` и `consumer_path`. Значения должны совпадать с `YT_PROXY`, `YT_POOL` и `YT_FLOW_ROOT`, использованными при bootstrap.

Проверить спеку:

```bash
MAIN=tech.ytsaurus.flow.examples.wordcount.WordCountApplication
docker run --rm --network host -v "$TASK/app:/app/pipeline" ghcr.io/ytsaurus/flow-java:0.3.0 \
    -cp 'build/install/wordcount/lib/*' "$MAIN" \
    --config pipeline.yson --validate-only > "$TASK/validate.log" 2>&1
```

Ожидается `Pipeline spec is valid`. Валидация выполняется без токена.

## 5. Запустить vanilla operation

```bash
docker run --rm --name "$RUN_ID-runner" --network host \
    -e YT_TOKEN -e YT_FLOW_WAIT=0 -v "$TASK/app:/app/pipeline" ghcr.io/ytsaurus/flow-java:0.3.0 \
    -cp 'build/install/wordcount/lib/*' "$MAIN" --config pipeline.yson
```
