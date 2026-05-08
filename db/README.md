# Gerador de Eventos Sintéticos

Este script Python gera eventos financeiros sintéticos em formato CSV, simulando o comportamento de clientes em um sistema bancário. Ele é útil para testes em sistemas baseados em eventos (Kafka), análise de fraude e simulações de fluxo de dados.

## O que o script faz

O script simula um período de dias (padrão: 6 dias) e gera diversos tipos de eventos para um grupo de clientes. Os eventos incluem:
- **TRANSACTION**: Transações financeiras (PIX, Crédito, Débito).
- **LOGIN**: Autenticações bem-sucedidas.
- **FAILED_AUTH**: Tentativas de login falhas.
- **DEVICE_CHANGE**: Cadastro de novos dispositivos confiáveis.
- **PASSWORD_CHANGE**: Alterações de senha.

Além dos eventos normais, o script injeta cenários específicos de comportamentos anômalos ou suspeitos em dias diferentes, facilitando o teste de regras de detecção de fraude.

## Variáveis Parametrizáveis

As seguintes variáveis podem ser ajustadas diretamente no código (`event_generator.py`) para alterar a simulação:

### Configurações de Clientes e Tempo
- `TOTAL_CLIENTS`: Quantidade total de clientes simulados (Padrão: 100).
- `DAYS_TO_GENERATE`: Número de dias de simulação (Padrão: 6).
- `BASE_DATE`: Data inicial da simulação (Padrão: 2026-05-04).
- `MAX_ACCOUNTS_PER_CLIENT`: Máximo de contas por cliente (Padrão: 2).
- `MAX_DEVICES_PER_CLIENT`: Máximo de dispositivos por cliente (Padrão: 2).

### Volume de Eventos Diários (Normais)
- `NORMAL_TX_VOLUME`: Volume de transações normais.
- `NORMAL_AUTH_VOLUME`: Volume de logins normais.
- `NORMAL_FAILED_AUTH_VOLUME`: Volume de falhas de autenticação normais.
- `NORMAL_DEVICE_CHANGE_VOLUME`: Volume de trocas de dispositivo normais.

### Cenários de Interesse (Anomalias)
- `S2_VALOR_ALTO_QTY`: Quantidade de clientes com transações de valor muito alto (dia 2).
- `S3_ALTA_FREQ_QTY`: Quantidade de clientes com alta frequência de transações em curto tempo (dia 3).
- `S4_DEVICE_NOVO_QTY`: Quantidade de clientes operando em dispositivos não reconhecidos (dia 4).
- `S5_SENHA_TX_QTY`: Quantidade de clientes com troca de senha seguida de transação (dia 5).
- `S6_FRAUDE_COMPLEXA_QTY`: Quantidade de cenários de fraude complexa (ataque de força bruta seguido de login e transação) (dia 6).

## Como rodar o script

Certifique-se de ter a biblioteca `pandas` instalada.

Para executar o gerador, utilize o seguinte comando:

```bash
python event_generator.py <nome_da_pasta_de_saida>
```

Substitua `<nome_da_pasta_de_saida>` pelo nome do diretório onde você deseja que os arquivos CSV sejam salvos. O script criará a pasta caso ela não exista.

### Exemplo:
```bash
python event_generator.py semana1
```

Ao final da execução, a pasta conterá arquivos `events_AAAA-MM-DD.csv` para cada dia simulado e um arquivo `customer_profiles.csv` com os perfis dos clientes gerados.
