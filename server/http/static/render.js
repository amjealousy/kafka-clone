const brokerPath = "/broker/v1";
const maxRenderedMessages = 300;
/** @type {EventSource} */
const activeConsumers = new Set();

function createElement(tag, className, text) {
    const node = document.createElement(tag);
    if (className) node.className = className;
    if (text !== undefined) node.textContent = text;
    return node;
}

function brokerURL(partition, action) {
    if (!partition.leader_alive || !partition.leader_http_address) {
        throw new Error("У партиции сейчас нет доступного лидера");
    }

    const address = partition.leader_http_address;
    const base = /^https?:\/\//i.test(address)
        ? address
        : `${window.location.protocol}//${address}`;
    return `${base.replace(/\/$/, "")}${brokerPath}/${action}`;
}

function bytesToBase64(value) {
    const bytes = new TextEncoder().encode(value);
    let binary = "";
    for (const byte of bytes) binary += String.fromCharCode(byte);
    return btoa(binary);
}

function base64ToText(value) {
    try {
        const binary = atob(value);
        const bytes = Uint8Array.from(binary, char => char.charCodeAt(0));
        return new TextDecoder().decode(bytes);
    } catch {
        return value;
    }
}

function formatTimestamp(value) {
    try {
        const milliseconds = BigInt(value) / 1_000_000n;
        return new Date(Number(milliseconds)).toLocaleString();
    } catch {
        return String(value || "—");
    }
}

function consumePartition(partition, topicName, handlers) {
    const query = new URLSearchParams({
        topic_name: topicName,
        partition_id: partition.partition_id,
        from_beginning: "true",
        till_end: "true",
    });
    const source = new EventSource(`${brokerURL(partition, "consume")}?${query}`);

    source.addEventListener("open", handlers.onOpen);
    source.addEventListener("message", event => {
        try {
            const payload = JSON.parse(event.data);
            if (payload.error) {
                source.close();
                handlers.onFatalError(new Error(payload.error));
                return;
            }
            handlers.onPayload(payload);
        } catch (error) {
            source.close();
            handlers.onFatalError(error);
        }
    });
    source.addEventListener("error", event => {
        // Именованное SSE-событие error содержит доменную ошибку сервера.
        if (event instanceof MessageEvent && event.data) {
            source.close();
            try {
                const payload = JSON.parse(event.data);
                handlers.onFatalError(new Error(payload.error || "Ошибка consume"));
            } catch (error) {
                handlers.onFatalError(error);
            }
            return;
        }
        // После сетевого сбоя EventSource сам попробует подключиться повторно.
        handlers.onReconnect();
    });
    return source;
}

async function produceMessage(partition, topicName, key, message) {
    const response = await fetch(brokerURL(partition, "produce"), {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify({
            topic_name: topicName,
            partition_id: Number(partition.partition_id),
            key,
            msg: bytesToBase64(message),
        }),
    });
    const result = await response.json().catch(() => ({}));
    if (!response.ok || result.status !== "Accepted") {
        throw new Error(result.status_message || `Produce завершился с HTTP ${response.status}`);
    }
}

function renderMessage(messageList, message, partitionID) {
    const empty = messageList.querySelector(".messages-empty");
    if (empty) empty.remove();

    const row = createElement("article", "message-row");
    const meta = createElement(
        "div",
        "message-meta",
        `Партиция ${partitionID} · offset ${message.offset} · ${formatTimestamp(message.timestamp)}`,
    );
    row.append(meta, createElement("pre", "message-body", base64ToText(message.msg || "")));
    messageList.append(row);

    while (messageList.children.length > maxRenderedMessages) {
        messageList.firstElementChild.remove();
    }
    messageList.scrollTop = messageList.scrollHeight;
}
function renderCreationTopicForm(form, onCreated) {
    const topicNameLabel = createElement(
        "label",
        "create-topic-label",
        "Название",
    );
    const topicNameInput = createElement("input", "create-topic-input");
    topicNameInput.name = "topic_name";
    topicNameInput.type = "text";
    topicNameInput.required = true;
    topicNameLabel.append(topicNameInput);

    const partitionsLabel = createElement(
        "label",
        "create-topic-label",
        "Партиции",
    );
    const partitionsInput = createElement("input", "create-topic-input");
    partitionsInput.name = "num_partitions";
    partitionsInput.type = "number";
    partitionsInput.min = "1";
    partitionsInput.value = "1";
    partitionsInput.required = true;
    partitionsLabel.append(partitionsInput);

    const replicationLabel = createElement(
        "label",
        "create-topic-label",
        "Replication factor",
    );
    const replicationInput = createElement("input", "create-topic-input");
    replicationInput.name = "replication_factor";
    replicationInput.type = "number";
    replicationInput.min = "1";
    replicationInput.value = "1";
    replicationInput.required = true;
    replicationLabel.append(replicationInput);

    const submitButton = createElement(
        "button",
        "create-topic-submit",
        "Создать топик",
    );
    submitButton.type = "submit";

    const statusOutput = createElement("output", "create-topic-status");
    statusOutput.setAttribute("aria-live", "polite");

    form.append(
        topicNameLabel,
        partitionsLabel,
        replicationLabel,
        submitButton,
        statusOutput,
    );

    form.addEventListener("submit", async event => {
        event.preventDefault();

        submitButton.disabled = true;
        statusOutput.textContent = "Создание…";
        statusOutput.className = "create-topic-status";

        try {
            const data = new FormData(form);

            const response = await fetch("/topic/v1/create", {
                method: "POST",
                headers: {"Content-Type": "application/json"},
                body: JSON.stringify({
                    topic_name: data.get("topic_name"),
                    num_partitions: Number(data.get("num_partitions")),
                    replication_factor: Number(
                        data.get("replication_factor"),
                    ),
                }),
            });

            const result = await response.json().catch(() => ({}));

            if (!response.ok) {
                throw new Error(
                    result.error_description ||
                    `Сервер вернул HTTP ${response.status}`,
                );
            }

            form.reset();
            partitionsInput.value = "1";
            replicationInput.value = "1";

            statusOutput.textContent = "Топик создан";
            statusOutput.className = "create-topic-status is-ok";

            try {
                await onCreated();
            } catch (error) {
                console.error("Не удалось обновить список топиков:", error);
                statusOutput.textContent = `Топик создан, но список не обновлён: ${error.message}`;
                statusOutput.className = "create-topic-status is-error";
            }
        } catch (error) {
            statusOutput.textContent = error.message;
            statusOutput.className = "create-topic-status is-error";
        } finally {
            submitButton.disabled = false;
        }
    });
}

function renderTopic(topic) {
    const partitions = topic.partitions || [];
    const card = createElement("article", "topic-card");
    const header = createElement("button", "topic-toggle");
    header.type = "button";
    header.setAttribute("aria-expanded", "false");

    const title = createElement("span", "topic-name", topic.name);
    const summary = createElement(
        "span",
        "topic-summary",
        `${partitions.length} парт. · RF ${topic.replication_factor}`,
    );
    const healthy = partitions.length > 0 && partitions.every(partition => partition.leader_alive);
    const status = createElement("span", `topic-status ${healthy ? "is-ok" : "is-error"}`, healthy ? "Работает" : "Проблема");
    const arrow = createElement("span", "topic-arrow", "⌄");
    header.append(title, summary, status, arrow);

    const details = createElement("section", "topic-details");
    details.hidden = true;
    const toolbar = createElement("div", "topic-toolbar");
    const partitionLabel = createElement("label", "partition-label", "Партиция");
    const partitionSelect = createElement("select", "partition-select");
    for (const partition of partitions) {
        const option = createElement("option", "", partition.partition_id);
        option.value = partition.partition_id;
        partitionSelect.append(option);
    }
    partitionLabel.append(partitionSelect);
    const streamStatus = createElement("span", "stream-status", "Поток остановлен");
    toolbar.append(partitionLabel, streamStatus);

    const messageList = createElement("div", "messages");
    messageList.setAttribute("aria-live", "polite");
    messageList.append(createElement("p", "messages-empty", "Сообщений пока нет"));

    const form = createElement("form", "produce-form");
    const keyInput = createElement("input", "produce-key");
    keyInput.name = "key";
    keyInput.placeholder = "Ключ (необязательно)";
    const messageInput = createElement("textarea", "produce-message");
    messageInput.name = "message";
    messageInput.placeholder = "Введите сообщение";
    messageInput.required = true;
    messageInput.rows = 2;
    const sendButton = createElement("button", "produce-submit", "Отправить");
    sendButton.type = "submit";
    const produceStatus = createElement("span", "produce-status");
    form.append(keyInput, messageInput, sendButton, produceStatus);
    details.append(toolbar, messageList, form);
    card.append(header, details);

    let consumer;
    const selectedPartition = () => partitions.find(
        partition => String(partition.partition_id) === partitionSelect.value,
    );

    const stopConsumer = () => {
        if (consumer) {
            consumer.close();
            activeConsumers.delete(consumer);
        }
        consumer = undefined;
    };

    const startConsumer = () => {
        stopConsumer();
        const partition = selectedPartition();
        if (!partition) return;
        messageList.replaceChildren(createElement("p", "messages-empty", "Сообщений пока нет"));
        streamStatus.textContent = "Подключение…";
        streamStatus.className = "stream-status";

        try {
            consumer = consumePartition(partition, topic.name, {
                onOpen: () => {
                    streamStatus.textContent = "SSE подключён";
                    streamStatus.className = "stream-status is-ok";
                },
                onPayload: payload => {
                    for (const message of payload.responses || []) {
                        renderMessage(messageList, message, partition.partition_id);
                    }
                },
                onReconnect: () => {
                    streamStatus.textContent = "Переподключение…";
                    streamStatus.className = "stream-status";
                },
                onFatalError: error => {
                    streamStatus.textContent = error.message;
                    streamStatus.className = "stream-status is-error";
                },
            });
            activeConsumers.add(consumer);
        } catch (error) {
            console.log("Got an error in renderTopic ")
            streamStatus.textContent = error.message;
            streamStatus.className = "stream-status is-error";
        }
    };

    header.addEventListener("click", () => {
        const expanded = header.getAttribute("aria-expanded") !== "true";
        header.setAttribute("aria-expanded", String(expanded));
        details.hidden = !expanded;
        arrow.textContent = expanded ? "⌃" : "⌄";
        if (expanded) startConsumer();
        else {
            stopConsumer();
            streamStatus.textContent = "Поток остановлен";
        }
    });

    partitionSelect.addEventListener("change", startConsumer);
    form.addEventListener("submit", async event => {
        event.preventDefault();
        const partition = selectedPartition();
        if (!partition || !messageInput.value) return;
        sendButton.disabled = true;
        produceStatus.textContent = "Отправка…";
        produceStatus.className = "produce-status";
        try {
            await produceMessage(partition, topic.name, keyInput.value, messageInput.value);
            messageInput.value = "";
            produceStatus.textContent = "Отправлено";
            produceStatus.className = "produce-status is-ok";
        } catch (error) {
            produceStatus.textContent = error.message;
            produceStatus.className = "produce-status is-error";
        } finally {
            sendButton.disabled = false;
        }
    });

    return card;
}

/**
 * @param {Object} overview JSON из GET /cluster/v1/overview.
 * @param {HTMLElement} root Контейнер админской панели.
 * @param refreshOverview
 */
export function renderControlPlaneView(
    overview,
    root = document.querySelector("#app"),
    refreshOverview = async () => {},
) {
    if (!root) throw new Error("Контейнер #app не найден");

    // Полная перерисовка удаляет карточки из DOM, но сама по себе не закрывает
    // принадлежащие им EventSource-соединения.
    for (const consumer of activeConsumers) consumer.close();
    activeConsumers.clear();
    root.replaceChildren();

    const heading = createElement("header", "cluster-heading");
    const controller = overview.controller
        ? `Контроллер: ${overview.controller.id}`
        : "Контроллер выбирается";
    heading.append(
        createElement("h1", "", "Kafka-clone"),
        createElement("p", "cluster-summary", `${controller} · Нод: ${overview.nodes?.length || 0} · Ответила нода: ${overview.served_by}`),
    );
    const creationTopicForm = createElement("form", "create-topic-form");

    creationTopicForm.append(
        createElement("h2", "", "Создать топик"),
    );

    renderCreationTopicForm(creationTopicForm, refreshOverview);
    const topicSection = createElement("section", "topics-section");
    topicSection.append(createElement("h2", "", "Топики"));
    const topics = overview.topics || [];
    if (topics.length === 0) {
        topicSection.append(createElement("p", "empty-state", "В кластере пока нет топиков"));
    } else {
        for (const topic of topics) topicSection.append(renderTopic(topic));
    }
    root.append(heading, creationTopicForm, topicSection);
}

// Старое имя оставлено, чтобы существующие вызовы не сломались.
export const renderControlPlateView = renderControlPlaneView;
