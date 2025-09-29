# PI-EngenhariaSoftware
Referências oficiais (para dicionários, downloads e dicionários de variáveis)

Microdados ENEM — INEP: https://www.gov.br/inep/pt-br/areas-de-atuacao/avaliacao-e-exames-educacionais/enem/microdados
Microdados Censo Escolar — INEP: https://www.gov.br/inep/pt-br/areas-de-atuacao/estatisticas-publicacoes-e-metricas/censo-escolar/microdados
Documentação LGPD e tratamento de dados (recomenda-se consulta ao jurídico da secretaria para diretrizes locais).

# Wireframe — Dashboard Analítico: ENEM + Censo Escolar
**Autor:** Julia Fonseca
**Objetivo:** Dashboard interativo para explorar correlações entre infraestrutura escolar, perfil socioeconômico e desempenho acadêmico (por município / Diretoria de Ensino).  
**Audience:** Diretoria de Ensino, Secretarias de Educação, Gestores escolares.

---

## Visão Geral
Painel web responsivo com:
- KPIs executivos no topo (médias por disciplina, variação inter-escola, índice socioeconômico médio).
- Filtros contextuais persistentes (ano, rede, diretoria, escola, faixa etária).
- Painéis de análise: correlação (scatter + trendline), boxplots por escola, mapa choropleth e ranking.
- Seção de storytelling com insights automáticos e recomendações acionáveis.
- Export (CSV/PDF) e controle de acesso (SSO + RBAC).

---

## Estrutura do Layout (High-level)
1. **Header (global)**  
   - Logotipo da Secretaria | Título: "Painel Educacional — Análise ENEM & Censo" | Usuário (avatar) + menu (Perfil / Sair)
2. **Barra lateral (left)** — Filtros persistentes (colapsável)
   - Ano (dropdown multi-select)
   - Rede (Municipal / Estadual / Privada)
   - Diretoria / Município (searchable dropdown)
   - Escola (autocomplete)
   - Indicadores socioeconômicos (slider / bucket)
   - Botões: Aplicar filtros | Reset
3. **Conteúdo principal (grid 2-col)**  
   - Linha 1 (KPIs cards horizontais) — 4 cards:
     - Média Geral (Matemática) — valor + delta vs ano anterior
     - Diferença média (com/sem laboratório)
     - Índice Socioeconômico Médio
     - Disparidade Intra-Escola (Desvio Padrão médio)
   - Linha 2 (analíticos)
     - Coluna A (65% width): Scatter plot (Índice Socioeconômico x Nota Média) com regressão e seleção por escola; control panel para toggle por disciplina.
     - Coluna B (35% width): Boxplots (Notas por escola) + tabela Top/Bottom 10.
   - Linha 3 (mapa & heatmap)
     - Mapa choropleth por escola/município (color = nota média) + tooltip com KPIs da escola.
     - Heatmap de correlação (infraestrutura x resultados).
   - Linha 4 (Insights & Actions)
     - Painel de storytelling automático (3-5 bullets) + recomendações priorizadas (investimento infra, formação docente).
     - Botões: Gerar Relatório PDF | Exportar CSV
4. **Footer**
   - Metadados: última atualização, volume de registros, versão do pipeline, link para repositório/lineage.

---

## Comportamentos Interativos
- Filtros devem persistir via URL (deep-link), permitindo compartilhar views.
- Cross-filtering: selecionar um ponto no scatter filtra a tabela, o mapa e os boxplots.
- Tooltip rico em gráficos com breadcrumbs para abrir página detalhada da escola.
- Histórico de comparações: permitir comparar até 3 escolas lado-a-lado.
- Acessibilidade: contraste WCAG AA, navegação por teclado e labels ARIA nos controles.

---

## Endpoints / DataSources
- **API Principal (read-only)** — Exemplo REST:
  - `GET /api/v1/aggregates?ano=2023&municipio=3524902&rede=municipal`
    - Retorna agregados por escola com campos: `co_escola, nome, codigo_municipio, avg_mt, avg_cn, avg_ch, avg_lc, avg_redacao, infra_index, socio_index, count_inscritos`
  - `GET /api/v1/school/{co_escola}/details?ano=2023`
    - Retorna série temporal e distribuição por aluno (anonimizada)
  - `GET /api/v1/map?ano=2023&nivel=municipio`
    - GeoJSON com propriedade `nota_media` por polígono
- **Formato recomendado:** JSON para APIs, Parquet/BigQuery para queries analíticas.
- **Autenticação:** OAuth2 / OpenID Connect (SSO institucional). RBAC junto ao campo `roles`.

---

## Segurança & Governança
- Remover PII e aplicar hashing com salt em `aluno_id`.
- Logs de auditoria (quem acessou qual relatório).
- Políticas de retenção e anonimização conforme LGPD.
- Documentar data lineage (origin, transforms, version).

---

## Notas de UX e Storytelling (visão)
- Priorizar insights acionáveis (ex.: “Escolas A, B e C com baixo infra_index e queda de -0.8σ em Matemática”).
- Cada insight deve mapear para uma ação operacional (treinamento docente, investimento em laboratório, conectividade).
- Incluir CTA (call to action) para exportar planos de intervenção e métricas de impacto.

