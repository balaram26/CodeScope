from project_assistant.services.service_factory import build_services


def main():
    services = build_services()
    required = [
        "llm_adapter",
        "project_helper",
        "project_import_service",
        "project_delete_service",
    ]
    for key in required:
        assert key in services, f"Missing service: {key}"
    print("Smoke test passed.")


if __name__ == "__main__":
    main()
