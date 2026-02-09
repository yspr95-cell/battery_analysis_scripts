import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots


def plot_cell_data(cell_df, ocv_table=None, skip_points=20, title='Cell Data Visualization'):
    """3-panel time series: Temperature, Voltage, Current."""
    fig = make_subplots(
        rows=3, cols=1, shared_xaxes=True,
        subplot_titles=("Temperature profile", "Voltage profile", "Current profile"),
        vertical_spacing=0.05
    )

    x_data = cell_df['Unix_datetime'][::skip_points]

    # Row 1: Temperature traces
    fig.add_trace(go.Scatter(x=x_data, y=cell_df['T_Chamber_degC'][::skip_points], name='T_Chamber_degC'), row=1, col=1)
    fig.add_trace(go.Scatter(x=x_data, y=cell_df['T_Cell_degC'][::skip_points], name='T_Cell_degC'), row=1, col=1)
    if 'T_cold_degC' in cell_df.columns:
        fig.add_trace(go.Scatter(x=x_data, y=cell_df['T_cold_degC'][::skip_points], name='T_cold_degC'), row=1, col=1)

    # Row 2: Voltage
    fig.add_trace(go.Scatter(x=x_data, y=cell_df['Voltage_V'][::skip_points], name='Voltage_V'), row=2, col=1)

    # Row 3: Current
    fig.add_trace(go.Scatter(x=x_data, y=cell_df['Current_A'][::skip_points], name='Current_A'), row=3, col=1)

    # OCV overlay if available
    if ocv_table is not None:
        fig.add_trace(go.Scatter(
            x=ocv_table['Unix_datetime'], y=ocv_table['Voltage_filt_V'],
            name='OCV extracted', mode='markers'
        ), row=2, col=1)

    fig.update_layout(title_text=title, template='plotly_white', height=600)
    fig.update_xaxes(title_text='Datetime', row=3, col=1, showticklabels=True)
    fig.update_yaxes(title_text='Temperatures (degC)', row=1, col=1)
    fig.update_yaxes(title_text='Voltage (V)', row=2, col=1)
    fig.update_yaxes(title_text='Current (A)', row=3, col=1)
    fig.update_layout(height=1080, width=1400, hovermode='x unified')
    fig.update_layout(hoversubplots="axis")

    return fig


def plot_dual_axis(df, x_col, y1_cols=[], y2_cols=[], skip_points=20, title=''):
    """Dual Y-axis plot with primary and secondary traces."""
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    x_data = df[x_col][::skip_points]

    for y_col in y1_cols:
        y_data = df[y_col][::skip_points]
        fig.add_trace(go.Scatter(x=x_data, y=y_data, name=y_col), secondary_y=False)

    for y_col in y2_cols:
        y_data = df[y_col][::skip_points]
        fig.add_trace(go.Scatter(x=x_data, y=y_data, name=y_col), secondary_y=True)

    fig.update_xaxes(title_text=x_col)
    fig.update_layout(title_text=title)
    fig.update_yaxes(title_text=" / ".join(filter(None, y1_cols)), secondary_y=False)
    fig.update_yaxes(title_text=" / ".join(filter(None, y2_cols)), secondary_y=True)

    return fig


def plot_ocv_vs_soc(ocv_table, cell_id):
    """OCV vs SOC colored by temperature, with repeat measurements as dotted/open-circle."""
    df_repeat_0 = ocv_table[ocv_table['Repeat'] == 0]
    df_repeat_gt_0 = ocv_table[ocv_table['Repeat'] > 0]

    fig = px.line(
        df_repeat_0,
        x='SOC_corrected',
        y='Voltage_filt_V',
        color='T_set',
        markers=True,
        title=f'OCV vs SOC : {cell_id}',
        labels={
            'SOC_corrected': 'SOC_actual (%)',
            'Voltage_filt_V': 'OCV (V)',
            'T_set': 'Temperature_set (degC)'
        }
    )

    for t_value in df_repeat_gt_0['T_set'].unique():
        df_subset = df_repeat_gt_0[df_repeat_gt_0['T_set'] == t_value]
        fig.add_trace(go.Scatter(
            x=df_subset['SOC_corrected'],
            y=df_subset['Voltage_filt_V'],
            mode='lines+markers',
            name=f'{t_value} (Repeat)',
            marker=dict(symbol='circle-open'),
            line=dict(dash='dot')
        ))

    fig.update_layout(template='plotly_white')
    return fig


def plot_QC_subplots(temp_df, title_text='', fig=None):
    """QC analysis 2-panel: Voltage/Current + Temperature/SOC with SOC reference lines."""
    if fig is None:
        fig = make_subplots(
            rows=2, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.1,
            specs=[[{"secondary_y": True}], [{"secondary_y": True}]]
        )

    time_ser = (temp_df['Unix_total_time'] - temp_df['Unix_total_time'].iloc[0]) / 60
    temp_df['Step_time_min'] = time_ser

    # Row 1: Voltage and Current
    fig.add_trace(
        go.Scatter(x=temp_df['Step_time_min'], y=temp_df['Voltage_V'],
                   name='Voltage (V)', line=dict(color='blue')),
        row=1, col=1, secondary_y=False
    )
    fig.add_trace(
        go.Scatter(x=temp_df['Step_time_min'], y=temp_df['Current_A'],
                   name='Current (A)', line=dict(color='red', dash='dash')),
        row=1, col=1, secondary_y=True
    )

    # Row 2: Temperature and SOC
    fig.add_trace(
        go.Scatter(x=temp_df['Step_time_min'], y=temp_df['T_Cell_degC'],
                   name='Cell Temp (degC)', line=dict(color='green')),
        row=2, col=1, secondary_y=False
    )
    fig.add_trace(
        go.Scatter(x=temp_df['Step_time_min'], y=temp_df['T_Chamber_degC'],
                   name='Chamber Temp (degC)', line=dict(color='darkturquoise')),
        row=2, col=1, secondary_y=False
    )
    fig.add_trace(
        go.Scatter(x=temp_df['Step_time_min'], y=temp_df['T_cold_degC'],
                   name='Cold-spot Temp (degC)', line=dict(color='cornflowerblue')),
        row=2, col=1, secondary_y=False
    )
    fig.add_trace(
        go.Scatter(x=temp_df['Step_time_min'], y=temp_df['SOC_corrected'],
                   name='SOC (%)', line=dict(color='orange', dash='dash')),
        row=2, col=1, secondary_y=True
    )

    # SOC reference lines
    soc_8_index = (temp_df['SOC_corrected'] - 8).abs().idxmin()
    soc_80_index = (temp_df['SOC_corrected'] - 80).abs().idxmin()
    date_8 = temp_df.loc[soc_8_index, 'Step_time_min']
    date_80 = temp_df.loc[soc_80_index, 'Step_time_min']

    min_temp = int(temp_df['T_Chamber_degC'].min()) - 1
    max_temp = max(min_temp + 15, int(temp_df['T_Cell_degC'].max())) + 1

    fig.update_layout(
        title=title_text,
        template='plotly_white',
        height=800,
        legend=dict(x=1.05),
        margin=dict(t=50, b=50),
        shapes=[
            dict(type='line', xref='x', yref='paper', x0=date_8, x1=date_8,
                 y0=0, y1=1, line=dict(color='purple', width=2, dash='dot')),
            dict(type='line', xref='x', yref='paper', x0=date_80, x1=date_80,
                 y0=0, y1=1, line=dict(color='purple', width=2, dash='dot'))
        ],
        annotations=[
            dict(x=date_8, y=-0.05, xref='x', yref='paper', text='SOC=8%',
                 showarrow=False, font=dict(color='purple')),
            dict(x=date_80, y=-0.05, xref='x', yref='paper', text='SOC=80%',
                 showarrow=False, font=dict(color='purple'))
        ]
    )

    fig.update_xaxes(title_text='Time (min)', showline=True, linewidth=1, linecolor='black', mirror=True, showticklabels=True, row=2, col=1)
    fig.update_xaxes(title_text='Time (min)', showline=True, linewidth=1, linecolor='black', mirror=True, showticklabels=True, row=1, col=1)
    fig.update_yaxes(title_text='Voltage (V)', showline=True, linewidth=1, linecolor='black', mirror=True, row=1, col=1, secondary_y=False)
    fig.update_yaxes(title_text='Current (A)', row=1, col=1, secondary_y=True)
    fig.update_yaxes(title_text='Temperature (degC)', range=[min_temp, max_temp], showline=True, linewidth=1, linecolor='black', mirror=True, row=2, col=1, secondary_y=False)
    fig.update_yaxes(title_text='SOC (%)', row=2, col=1, secondary_y=True)

    return fig


def plot_T_estimate_for_Ceff(time, temperature, Ta, c_eff, title_text, Qgen=None):
    """Thermal fit plot: actual vs estimated temperature."""
    from thermal import simulate_T_from_Ceff_Qgen

    temperature = np.array(temperature)
    time = np.array(time)
    initial_temperature = temperature[0]
    estimated_temperature = simulate_T_from_Ceff_Qgen(time, initial_temperature, Ta, c_eff, Qgen)

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=time, y=temperature, mode='markers', name='Actual Temperature'))
    fig.add_trace(go.Scatter(x=time, y=estimated_temperature, mode='markers', name='Estimated Temperature'))
    fig.update_layout(
        title=title_text,
        xaxis_title='Time',
        yaxis_title='Temperature',
        legend_title='Legend'
    )
    return fig
