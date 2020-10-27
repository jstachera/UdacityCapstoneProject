def _get_dict_path(isLocal: bool):
    return r'i94project\dicts\\' if isLocal else r's3a://i94project/dicts//'


def _get_dict_input(isLocal: bool, file_name: str):
    return f"{_get_dict_path(isLocal)}{file_name}"


def _get_stage_input(isLocal: bool, file_name: str):
    stage_input = r'i94project\stage\input\\' if isLocal else r's3a://i94project/stage/input//'
    return f"{stage_input}{file_name}"


def _get_stage_output(isLocal: bool, file_name: str, output_format: str):
    stage_output = r'i94project\stage\output\\' if isLocal else r's3a://i94project/stage/output//'
    return f"{stage_output}{file_name}.{output_format}"


def get_paths(isLocal: bool, dataset: str, output_format: str) -> ():
    if "airport" in dataset:
        return (_get_dict_path(isLocal),
                _get_stage_input(isLocal, 'airport-codes.csv'),
                _get_stage_output(isLocal, 'airport', output_format))
    elif 'temperature' in dataset:
        return (_get_dict_path(isLocal),
                _get_stage_input(isLocal, 'GlobalLandTemperaturesByCountry.csv'),
                _get_stage_output(isLocal, 'temperature', output_format))
    elif 'country' in dataset:
        return (_get_dict_path(isLocal),
                _get_stage_input(isLocal, 'GlobalLandTemperaturesByCountry.csv'),
                _get_dict_input(isLocal, 'country_code.csv'),
                _get_stage_output(isLocal, 'country', output_format))
    elif 'us_state' in dataset:
        return (_get_dict_path(isLocal),
                _get_stage_input(isLocal, 'us-cities-demographics.csv'),
                _get_dict_input(isLocal, 'us_state.csv'),
                _get_stage_output(isLocal, 'us_state', output_format))
    elif 'immigration' in dataset:
        return (_get_dict_path(isLocal),
                _get_stage_input(isLocal, 'i94_apr16_sub.csv'),
                _get_stage_output(isLocal, 'date', output_format),
                _get_stage_output(isLocal, 'immigration', output_format))
    else:
        raise ValueError(f"Unknown dataset: {dataset}!")
