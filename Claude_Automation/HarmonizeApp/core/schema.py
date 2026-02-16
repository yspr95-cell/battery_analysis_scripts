"""Target schema definition for battery test data harmonization."""

FOCUS_COLS_ETL = [
    'Total_time_s',
    'Date_time',
    'Unix_time',
    'Step',
    'Step_name',
    'Cycle',
    'Voltage_V',
    'Current_A',
    'Power_W',
    'Capacity_step_Ah',
    'Energy_step_Wh',
    'T_Cell_degC',
    'T_Anode_degC',
    'T_Cathode_degC',
    'T_Chamber_degC',
    'T_cold_degC',
]

MANDATORY_COLS_ETL = [
    'Total_time_s',
    'Date_time',
    'Voltage_V',
    'Current_A',
    'Capacity_step_Ah',
]

COLUMN_METADATA = {
    'Total_time_s':     {'unit': 's',    'dtype': 'float64', 'description': 'Total elapsed test time'},
    'Date_time':        {'unit': None,   'dtype': 'datetime64[ns]', 'description': 'Absolute timestamp'},
    'Unix_time':        {'unit': 's',    'dtype': 'float64', 'description': 'Unix epoch timestamp'},
    'Step':             {'unit': None,   'dtype': 'int64',   'description': 'Step index number'},
    'Step_name':        {'unit': None,   'dtype': 'str',     'description': 'Charge/Discharge/Rest/Control'},
    'Cycle':            {'unit': None,   'dtype': 'int64',   'description': 'Cycle number'},
    'Voltage_V':        {'unit': 'V',    'dtype': 'float64', 'description': 'Cell voltage'},
    'Current_A':        {'unit': 'A',    'dtype': 'float64', 'description': 'Cell current'},
    'Power_W':          {'unit': 'W',    'dtype': 'float64', 'description': 'Cell power'},
    'Capacity_step_Ah': {'unit': 'Ah',   'dtype': 'float64', 'description': 'Step capacity'},
    'Energy_step_Wh':   {'unit': 'Wh',   'dtype': 'float64', 'description': 'Step energy'},
    'T_Cell_degC':      {'unit': 'degC', 'dtype': 'float64', 'description': 'Cell temperature'},
    'T_Anode_degC':     {'unit': 'degC', 'dtype': 'float64', 'description': 'Anode temperature'},
    'T_Cathode_degC':   {'unit': 'degC', 'dtype': 'float64', 'description': 'Cathode temperature'},
    'T_Chamber_degC':   {'unit': 'degC', 'dtype': 'float64', 'description': 'Chamber temperature'},
    'T_cold_degC':      {'unit': 'degC', 'dtype': 'float64', 'description': 'Cold-side temperature'},
}
