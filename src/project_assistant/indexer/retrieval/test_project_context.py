import argparse

from project_assistant.indexer.retrieval.project_context_builder import (
    build_project_context,
    render_project_context_text,
)


def main():
    parser = argparse.ArgumentParser(description="Build assistant-ready project retrieval context.")
    parser.add_argument("--project-name", required=True)
    parser.add_argument("--query", required=True)
    parser.add_argument("--index-path", required=True)
    parser.add_argument("--meta-path", required=True)
    parser.add_argument("--embedding-model", default="BAAI/bge-base-en-v1.5")
    parser.add_argument("--top-k", type=int, default=6)
    args = parser.parse_args()

    ctx = build_project_context(
        project_name=args.project_name,
        query=args.query,
        index_path=args.index_path,
        meta_path=args.meta_path,
        embedding_model=args.embedding_model,
        top_k=args.top_k,
    )
    print(render_project_context_text(ctx))


if __name__ == "__main__":
    main()
