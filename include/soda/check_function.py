def check(scan_name, file_checks_yml, configuration_file_yml, data_source, project_root='/usr/local/airflow/include'):
    from soda.scan import Scan
    print("Running Soda Scan ...")
    config_file = f"{project_root}/soda/{configuration_file_yml}"
    checks_path = f"{project_root}/soda/checks"
    
    scan = Scan()
    scan.set_verbose()
    scan.add_configuration_yaml_file(config_file)
    scan.set_data_source_name(data_source)
    
    if '.yml' in file_checks_yml:
        scan.add_sodacl_yaml_file(f"{checks_path}/{file_checks_yml}")
    else:
        scan.add_sodacl_yaml_files(checks_path)
    
    scan.set_scan_definition_name(scan_name)
    
    result = scan.execute()
    print(scan.get_logs_text())
    
    if result != 0:
        print(f"Soda Scan failed with exit code: {result}")
        return {"test": scan_name, "status": "failed", "result": result}
    else:
        print("Soda Scan completed successfully")
        return {"test": scan_name, "status": "success", "result": result}