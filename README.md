# ⚙️ Automação Financeira Integrada – Power BI + Excel + Conta Azul + RPA

> 🔹 *Sistema completo de automação financeira que integra Excel, Power BI, Conta Azul e RPA para controle de inadimplência, atualização de datasets e gestão de restituições.*

---

## 🚀 **Contexto do Projeto**

Me foi demandado um projeto com o seguinte cenário:

Um arquivo **Excel para alimentação do próprio gestor**, de forma que conseguisse **conectar um painel Power BI a ele**, mantendo tudo **atualizado automaticamente**.

Com esse cenário, escolhi o seguinte:
- Um **arquivo Excel dentro do Dropbox**, ferramenta de armazenamento em nuvem usada pela empresa;
- Conexão por link direto com o **Power BI**;
- Uma **RPA com Selenium** responsável por atualizar o dataset do Power BI na web, garantindo que o painel refletisse sempre a versão mais recente;
- E uma **rotina automática com o Agendador de Tarefas** rodando a cada **2 minutos**, alcançando as necessidades operacionais e de mercado que me foram passadas.

Além disso, foi solicitada uma **solução de painel para gestão de clientes inadimplentes**, integrando a **API do Conta Azul** com base de dados manual, para que a equipe de atendimento tivesse **visão direta dos clientes não pagantes** e pudesse agir de forma proativa.

---

## 🧩 **Componentes Técnicos**

### 🟦 `ContaAzul.py`
Script responsável por:
- Conectar-se à **API Conta Azul**, utilizando autenticação OAuth2;
- Extrair dados de **contas a receber em atraso** e da **base de clientes**;
- Tratar, normalizar e unir informações de clientes e recebíveis;
- Atualizar o banco de dados **PostgreSQL** com os dados tratados;
- Integrar e sincronizar o status dos clientes com uma base no **Notion** (API Notion);
- Converter e formatar valores financeiros em padrão BRL → float;
- Gerar dataset limpo e estruturado, pronto para o **Power BI**.

**Principais funções:**
- `renovar_access_token()` → Atualiza automaticamente o token de acesso à API;
- `buscar_contas_a_receber()` → Busca todas as contas vencidas;
- `buscar_clientes()` → Lista todos os clientes ativos e inativos;
- `comparar_nomes()` → Cruza nomes da base do Conta Azul com o Notion para identificar inadimplentes;
- `comparacao_notion()` → Atualiza o status de “Inadimplente?” no Notion;
- `conectar_banco()` → Conecta e escreve os dados tratados no PostgreSQL.

---

### 🟨 `obser_excel.py`
Script responsável por:
- **Monitorar automaticamente arquivos Excel** críticos (como `Gestão de Restituições.xlsx` e `Conta_azul.xlsm`);
- Detectar qualquer **modificação, criação ou exclusão** de arquivos;
- Disparar automaticamente o processo de **atualização do Power BI**;
- Utilizar o **Watchdog** com sistema de *debounce* e *cooldown* (para evitar múltiplas execuções simultâneas);
- Caso o watchdog falhe, entra em **modo fallback com polling**;
- Enviar **notificações via Slack** para acompanhamento de execução (“Atualização detectada”, “Power BI atualizado”, “Falha”, etc).

**Principais funções:**
- `file_hash()` → Calcula hash SHA-256 do arquivo para detectar mudanças;
- `DebouncedRunner()` → Garante intervalos controlados entre execuções;
- `MultiFileHandler()` → Observa múltiplos diretórios e identifica alterações;
- `start_watchdog()` → Inicializa a observação principal;
- `start_polling()` → Ativa o modo de verificação por loop, caso o watchdog falhe;
- `main()` → Gerencia o ciclo de vida completo da automação.

---

## 🧰 **Stack Utilizada**

| Categoria | Tecnologias |
|------------|--------------|
| **Linguagem** | Python 3.12 |
| **Bibliotecas Principais** | `pandas`, `requests`, `selenium`, `sqlalchemy`, `watchdog`, `threading`, `hashlib`, `json`, `os`, `re` |
| **Banco de Dados** | PostgreSQL |
| **APIs Integradas** | Conta Azul API, Notion API, Slack Webhook |
| **Visualização** | Power BI (via conexão direta e link Dropbox) |
| **Automação** | Selenium + Windows Task Scheduler |
| **Infraestrutura** | Dropbox + Rede Corporativa |
| **Validação** | Excel (com macros e validação de dados) |

---

## 🧠 Regras de Negócio Aplicadas

O painel de inadimplência foi estruturado seguindo o modelo fato x dimensão, garantindo integridade e flexibilidade:

| Aba             | Função                                                      | Automação                                      |
| --------------- | ----------------------------------------------------------- | ---------------------------------------------- |
| **Verificação** | Puxa os nomes da base API e compara com a planilha anterior | Macro de verificação + API Conta Azul          |
| **Consulta**    | Exibe tabela dinâmica com colunas de observações manuais    | Atualização automática com consistência        |
| **Macros**      | Identificam novos, ausentes e reincidentes                  | Colorem células e apagam campos inconsistentes |
| **RPA**         | Atualiza dataset Power BI                                   | Executada a cada 2 minutos                     |

## 📈 Impacto Estratégico

1️⃣ Time Financeiro

* Restituição: acompanha o lucro real da empresa, avaliando se as margens estão adequadas e onde há oportunidades de reinvestimento.

* Inadimplência: identifica quem precisa ser cobrado, quanto falta receber e quais medidas preventivas devem ser tomadas.

2️⃣ Time Operacional

* Painéis atualizados sem intervenção manual;

* Comunicação automatizada via Slack;

* Redução de gargalos e retrabalho;

* Tomada de decisão com base em dados confiáveis e em tempo real.

