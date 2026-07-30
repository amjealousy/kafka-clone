# создать топик (запрос уйдёт на текущего контроллера автоматически)
`./kafka-client topic create --control 127.0.0.1:7090 --topic test-topic --partitions 3 --replication-factor 3`

# посмотреть метаданные (лидер/реплики по партициям)
`./kafka-client topic describe --control 127.0.0.1:7090 --topic test-topic`

# produce — сам найдёт лидера партиции через DescribeTopic
`./kafka-client produce --control 127.0.0.1:7090 --topic test-topic --partition 0 --key my-key --message "hello"`

# consume — сам найдёт in-sync реплику
`./kafka-client consume --control 127.0.0.1:7090 --topic test-topic --partition 0 --from-beginning`

# tail-режим (не закрывать соединение, ждать новые сообщения)
`./kafka-client consume --control 127.0.0.1:7090 --topic test-topic --partition 0 --follow`