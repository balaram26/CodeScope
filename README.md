# CodeScope

**Understand entire projects — code, workflows, and results.**

CodeScope is built for real-world projects — where logic is spread across code, workflows, configs, and outputs — not just clean codebases.

It builds a structured understanding of your project and lets you ask questions grounded in actual project evidence.
---

## 🚀 Why this is different

Most tools:
- focus only on source code
- assume clean repositories
- ignore results, configs, and workflows

**CodeScope is built for real projects.**

It works across:
- scripts
- pipelines (Nextflow, shell, etc.)
- config files
- markdown notes
- result files and tables

It reconstructs **how a project actually works**, not just what individual files contain.

---

## 🧠 What it does

- 📦 Import a project (folder or zip)
- 🔍 Parse mixed file types (Python, R, Nextflow, YAML, Markdown, etc.)
- 🧬 Extract structure and function-level metadata
- 🧾 Generate summaries and file-level dossiers
- 🧩 Build a unified project index
- ❓ Answer questions using retrieved project evidence

---

## 🧾 Supported File Types

CodeScope is designed to work with **mixed project repositories** and currently supports parsing:

### 🧑‍💻 Code

* Python (`.py`)
* R (`.R`, `.r`)
* C/C++ (`.cpp`, `.h`)

### 🔁 Workflows & Scripts

* Shell scripts (`.sh`)
* Nextflow pipelines (`.nf`)
* Nextflow config (`nextflow.config`)

### ⚙️ Configuration & Data

* YAML (`.yaml`, `.yml`)
* JSON (`.json`)
* Tabular files (`.csv`, `.tsv`)

### 📄 Documentation

* Markdown (`.md`)
* Dockerfiles

---

## ⚙️ How it works

Each file type is handled by a specialized parser that extracts:

* structure (functions, blocks, steps)
* metadata (names, parameters, relationships)
* execution logic (for scripts and workflows)

This information is later used to build a unified project index.

---

## ⚠️ Notes

* Parsing coverage varies by file type
* Some formats (like scripts and workflows) are interpreted structurally rather than fully semantically
* Support for additional languages and formats is planned

---
## 💡 Behavior on unsupported files

```md
If a file type is not explicitly supported, it may still be indexed as raw text.
```
---

## 💡 Use cases

- Multiple projects running in parallel
- Understanding unfamiliar pipelines
- Reverse-engineering old research projects
- Exploring large mixed repositories
- Tracing how results were generated

Ask questions like:

- What does this project actually do?
- Which scripts generate these results?
- How are these files connected?
- Where is this logic implemented?

---

## ⚙️ Installation

```bash
git clone <your-repo-url>
cd project-intelligence
pip install -e .
pip install -r requirements.txt
```

---

## 🔧 Configuration

CodeScope uses a local model configuration:

```bash
export PROJECT_ASSISTANT_MODELS_YAML=/path/to/models.local.yaml
```

Example config:

```
models/models.example.yaml
```

---

## ▶️ Run the app

```bash
PYTHONPATH=src streamlit run src/project_assistant/streamlit_app.py
```

---

## 🧪 Quickstart

1. Launch the app
2. Import a project (folder or zip)
3. Wait for indexing to complete
4. Go to **Ask**
5. Start querying your project

## 🧪 Built-in Demo (This Repository Itself)

CodeScope includes a **pre-indexed demo dataset based on this repository itself**.

This means:

* The current database and indexed chunks were generated from this project
* You can start the app and immediately explore how CodeScope works
* You can ask questions about the **internal architecture and code of CodeScope**

### 👉 Try it

After launching the app:

1. Go to **Ask**
2. Select the available project (e.g., `indexer` or similar)
3. Ask questions like:

* What does this project do overall?
* How does the indexing pipeline work?
* Which modules handle parsing?
* How are embeddings and FAISS used?

---

### 💡 Why this is useful

This serves as both:

* a **live demo**
* a **reference example** of how CodeScope indexes and understands a real project

You can explore the system using its own indexed knowledge.

---

### ⚠️ Note

* The demo data reflects a snapshot of this repository
* If you modify the code, you may want to re-index for updated results

---

## 🧱 Architecture

```
src/project_assistant/
  ai/            → local LLM + embeddings
  indexer/       → ingestion → parsing → IR → LLM → chunks → index
  services/      → orchestration layer
  streamlit_app  → UI
```

Pipeline:

```
Ingest → Parse → Deduplicate → IR → LLM → Merge → Summaries → Chunks → Index
```

---

## 📊 What gets indexed

- Source code (functions, structure)
- Scripts and workflows
- Configurations
- Documentation
- Generated summaries
- Project-level artifacts

---

## ⚠️ Current limitations

- Indexing large projects can take time
- Performance depends on local model and resources.
- Some file types may have partial support
- First open-source release focuses on core pipeline

---

## 🛣️ Roadmap

- Faster incremental indexing
- Better cross-file linking
- Visual project graphs
- Improved evidence ranking
- Expanded parser coverage

---

## 📜 License

MIT
