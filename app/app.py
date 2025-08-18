import os
from databricks import sql
import pandas as pd
import numpy as np
from shapely.geometry import Polygon
import dash
from dash import dcc, html, Input, Output, State, callback_context, no_update
import plotly.express as px
import dash_bootstrap_components as dbc
import dash_ag_grid as dag
from databricks.sdk.core import Config
import dash_leaflet as dl
from dash.dependencies import Input, Output
from dash_extensions.javascript import arrow_function
from flask import request
import json
import h3
import datetime as dt
import time

# Set up the app
app = dash.Dash(__name__)



# Check for environment variables but don't fail if they're not set (for development)
DATABRICKS_WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID")
# DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

global_center = None
global_zoom = None
global_bounds = None

if not DATABRICKS_WAREHOUSE_ID:
    print("Warning: DATABRICKS_WAREHOUSE_ID not set. Cannot pull data.")

def get_databricks_token():
    DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
    if not DATABRICKS_TOKEN:
        print("DATABRICKS_TOKEN not set in environment variables, using on-behalf-of authentication.")
        DATABRICKS_TOKEN = request.headers.get("x-forwarded-access-token")
    return DATABRICKS_TOKEN

# if not DATABRICKS_TOKEN:
#     print("DATABRICKS_TOKEN not set in environment variables, using on-behalf-of authentication.")
#     DATABRICKS_TOKEN = request.headers.get("x-forwarded-access-token")
#     # # Not running inside Databricks App or not authenticated
#     # df = pd.DataFrame({"error": ["Not authenticated"]})
#     # return px.bar(df, x="error", y=None)

def sqlQuery(query: str) -> pd.DataFrame:
    """Execute a SQL query and return the result as a pandas DataFrame."""
    with sql.connect(
        http_path=f"/sql/1.0/warehouses/{DATABRICKS_WAREHOUSE_ID}",
        server_hostname="e2-demo-field-eng.cloud.databricks.com",
        access_token=get_databricks_token()
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            df = pd.DataFrame(rows, columns=columns)
        return df

# def get_mock_data():
#     """Generate mock data for development when database is not available"""
#     print("Generating mock data for development...")
    
#     # Create some mock hexagons around NYC
#     mock_hexagons = [
#         "85283473fffffff",  # NYC area H3 hexagon
#         "85283477fffffff",
#         "8528347bfffffff",
#         "85283483fffffff",
#         "85283487fffffff",
#         "8528348bfffffff",
#         "8528348ffffffff",
#         "85283493fffffff",
#         "85283497fffffff"
#     ]
    
#     mock_data = []
#     for i, hex_id in enumerate(mock_hexagons):
#         # Generate mock boundary and center
#         try:
#             hex_boundary = h3.cell_to_boundary(hex_id)
#             hex_center = h3.cell_to_latlng(hex_id)
            
#             # Convert boundary to GeoJSON format
#             boundary_coords = [[coord[1], coord[0]] for coord in hex_boundary]  # Convert to [lng, lat]
            
#             mock_data.append({
#                 'hex_id': hex_id,
#                 'hex_boundary': json.dumps({
#                     "type": "Polygon",
#                     "coordinates": [boundary_coords]
#                 }),
#                 'hex_center': json.dumps({
#                     "type": "Point",
#                     "coordinates": [hex_center[1], hex_center[0]]  # GeoJSON format: [lng, lat]
#                 }),
#                 'count': np.random.randint(10, 500)  # Random count for demo
#             })
#         except Exception as e:
#             print(f"Error generating mock data for hex {hex_id}: {e}")
#             continue
    
#     return pd.DataFrame(mock_data)

# Fetch the all h3 data
def get_data(catalog=None, schema=None, table=None, column=None, resolution=9, bounds=None, column_resolution=None):
    stime = dt.datetime.now()
    
    if not catalog or not schema or not table or not column:
        print("No catalog, schema, table, or column provided. Returning empty data.")
        return []
    
    # print(f"USE_MOCK_DATA: {USE_MOCK_DATA}")
    # if USE_MOCK_DATA:
    #     data = [get_mock_data()]
    # else:
    try:
        bounds_wkt = bounds_to_wkt(bounds) if bounds else None
        # resolution_query = f"SELECT h3_resolution({column}) as resolution FROM {catalog}.{schema}.{table} LIMIT 1"
        # resolution_query_result = sqlQuery(resolution_query)['resolution'].iloc[0]
        # print(f"resolution_query_result: {resolution_query_result}")
        # resolution = min([resolution_query_result, resolution])
        resolution = min([int(column_resolution), resolution])
        print(f"resolution: {resolution}")
        print(f"RESOLUTION QUERY TOOK:    {dt.datetime.now() - stime}")
        stime = dt.datetime.now()

        query = f"""
                    WITH cell_agg AS (
                    SELECT
                        h3_toparent({column}, {resolution}) as h3_cell_id,
                        count(*) as count
                    FROM {catalog}.{schema}.{table}
                    WHERE {f"h3_toparent({column}, {resolution}) IN (SELECT EXPLODE(H3_COVERASH3('{bounds_wkt}', {resolution})))" if bounds_wkt else "1=1"}
                    GROUP BY h3_cell_id
                    )
                    SELECT h3_boundaryasgeojson(h3_cell_id) as hex_boundary,
                            count
                    FROM cell_agg
                    ORDER BY count DESC
        """
        # query = f"""
        #             WITH cell_agg AS (
        #             SELECT
        #                 h3_toparent(pickup_cell_12, {resolution}) as h3_cell_id,
        #                 count(*) as count
        #             FROM mjohns.liquid_nyc_h3_trip.h3_taxi_trips
        #             WHERE {f"h3_toparent(pickup_cell_12, {resolution}) IN (SELECT EXPLODE(H3_COVERASH3('{bounds_wkt}', {resolution})))" if bounds_wkt else "1=1"}
        #             GROUP BY h3_cell_id
        #             )
        #             SELECT h3_boundaryasgeojson(h3_cell_id) as hex_boundary,
        #                     count
        #             FROM cell_agg
        #             ORDER BY count DESC
        # """
        # query = f"""
        #             WITH cell_agg AS (
        #             SELECT
        #                 h3_toparent(h3, {resolution}) as h3_cell_id,
        #                 count(*) as count
        #             FROM justinm.geospatial.cb_walk_silver
        #             WHERE {f"h3_toparent(h3, {resolution}) IN (SELECT EXPLODE(H3_COVERASH3('{bounds_wkt}', {resolution})))" if bounds_wkt else "1=1"}
        #             GROUP BY h3_cell_id
        #             )
        #             SELECT h3_boundaryasgeojson(h3_cell_id) as hex_boundary,
        #                     count
        #             FROM cell_agg
        #             ORDER BY count DESC
        # """
        print(query)
        data = sqlQuery(query)
        print(data.head())
        # Convert any ndarray columns to lists
        for col in data.columns:
            if isinstance(data[col].iloc[0], np.ndarray):
                data[col] = data[col].apply(list)
    except Exception as e:
        print(f"An error occurred in querying data: {str(e)}")
        print("Falling back to mock data...")
        data = []
    
    print(f"DATA QUERY TOOK:    {dt.datetime.now() - stime}")
    print(data.head())
    print(len(data), "rows")
    return data

def get_catalogs():
    with sql.connect(
        http_path=f"/sql/1.0/warehouses/{DATABRICKS_WAREHOUSE_ID}",
        server_hostname="e2-demo-field-eng.cloud.databricks.com",
        access_token=get_databricks_token()
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SHOW CATALOGS")
            catalogs = cursor.fetchall()
            catalogs = [catalog.catalog for catalog in catalogs]
            # print(catalogs)
        return catalogs
    
def get_schemas(catalog):
    with sql.connect(
        http_path=f"/sql/1.0/warehouses/{DATABRICKS_WAREHOUSE_ID}",
        server_hostname="e2-demo-field-eng.cloud.databricks.com",
        access_token=get_databricks_token()
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"SHOW SCHEMAS IN {catalog}")
            schemas = cursor.fetchall()
            schemas = [schema.databaseName for schema in schemas]
            print(f"SCHEMAS in {catalog}: {schemas}")
        return schemas

def get_tables(catalog, schema):
    with sql.connect(
        http_path=f"/sql/1.0/warehouses/{DATABRICKS_WAREHOUSE_ID}",
        server_hostname="e2-demo-field-eng.cloud.databricks.com",
        access_token=get_databricks_token()
    ) as connection:
        with connection.cursor() as cursor: 
            cursor.execute(f"SHOW TABLES IN {catalog}.{schema}")
            tables = cursor.fetchall()
            tables = [table.tableName for table in tables]
            print(f"TABLES IN {catalog}.{schema}: {tables}")
        return tables

def get_columns(catalog, schema, table):
    with sql.connect(
        http_path=f"/sql/1.0/warehouses/{DATABRICKS_WAREHOUSE_ID}",
        server_hostname="e2-demo-field-eng.cloud.databricks.com",
        access_token=get_databricks_token()
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"SHOW COLUMNS IN {catalog}.{schema}.{table}")
            columns = cursor.fetchall()
            columns = [col.col_name for col in columns]
            print(f"COLUMNS IN {catalog}.{schema}.{table}: {columns}")
        return columns

def style_function(count, septiles):
    fill_color = '#FFFFFF'  # default white
    
    if count >= septiles[6]:
        fill_color = '#800026'      # dark red
    elif count >= septiles[5]:
        fill_color = '#BD0026'      # red
    elif count >= septiles[4]:
        fill_color = '#E31A1C'      # light red
    elif count >= septiles[3]:
        fill_color = '#FC4E2A'      # orange-red
    elif count >= septiles[2]:
        fill_color = '#FD8D3C'      # orange
    elif count >= septiles[1]:
        fill_color = '#FEB24C'      # light orange
    elif count >= septiles[0]:
        fill_color = '#FED976'      # yellow
    
    return {
        "fillColor": fill_color,
        "weight": 1,
        "opacity": 0.9,
        "color": fill_color,
        "fillOpacity": 0.7
    };

def create_legend(septiles):
    """Create a legend component for the map"""
    legend_items = [
        {"color": "#FED976", "label": f"< {int(septiles[1])}"},
        {"color": "#FEB24C", "label": f"{int(septiles[1])}-{int(septiles[2])}"},
        {"color": "#FD8D3C", "label": f"{int(septiles[2])}-{int(septiles[3])}"},
        {"color": "#FC4E2A", "label": f"{int(septiles[3])}-{int(septiles[4])}"},
        {"color": "#E31A1C", "label": f"{int(septiles[4])}-{int(septiles[5])}"},
        {"color": "#BD0026", "label": f"{int(septiles[5])}-{int(septiles[6])}"},
        {"color": "#800026", "label": f"≥ {int(septiles[6])}"}
    ]
    
    legend_divs = []
    for item in legend_items:
        legend_divs.append(
            html.Div([
                html.Div(
                    style={
                        "backgroundColor": item["color"],
                        "width": "20px",
                        "height": "20px",
                        "border": "1px solid #000",
                        "display": "inline-block",
                        "marginRight": "8px"
                    }
                ),
                html.Span(item["label"], style={"fontSize": "12px", "color": "#FFFFFF"})
            ], style={"marginBottom": "5px"})
        )
    
    return html.Div([
        html.Div("Counts",
                 style={"marginBottom": "10px",
                        "fontSize": "14px",
                        "color": "#FFFFFF",
                        "fontFamily": "Helvetica",
                        "fontWeight": "bold"}),
        html.Div(legend_divs)
    ], 
                #     style={
                #     "backgroundColor": "#3A3A3A",
                #     "color": "#FFFFFF",
                #     "cursor": "pointer",
                #     "borderRadius": "4px",
                #     "fontSize": "14px",
                #     "fontFamily": "Helvetica",
                #     "position": "absolute",
                #     "top": "10px",
                #     "right": "10px",
                #     "zIndex": "1000",
                #     "padding": "8px 16px"
                # }
                style={
        "position": "absolute",
        "top": "10px",
        "right": "10px",
        "backgroundColor": "#3A3A3A",
        "padding": "8px 16px", #"10px",
        "borderRadius": "4px",
        "boxShadow": "0 0 10px rgba(0,0,0,0.3)",
        "zIndex": "1000",
        "fontFamily": "Helvetica"
    }
    )

def zoom_to_h3_resolution(zoom):
    if zoom < 4:
        return 5
    elif zoom < 8:
        return 6
    elif zoom < 10:
        return 7
    elif zoom < 12:
        return 8
    elif zoom < 13:
        return 9
    elif zoom < 14:
        return 10
    elif zoom < 16:
        return 11
    elif zoom < 17:
        return 12
    else:
        return 12
    
def bounds_to_wkt(bounds):
    # Input bounds: [southwest, northeast] in (lat, lon)
    sw = bounds[0]  # [lat, lon]
    ne = bounds[1]  # [lat, lon]

    # Create polygon corners: lon/lat order
    polygon_coords = [
        (sw[1], sw[0]),  # lower left
        (sw[1], ne[0]),  # upper left
        (ne[1], ne[0]),  # upper right
        (ne[1], sw[0]),  # lower right
        (sw[1], sw[0])   # close polygon
    ]

    # Create polygon and convert to WKT
    poly = Polygon(polygon_coords)
    wkt_string = poly.wkt
    return wkt_string

def create_log_color_scale(data, n_colors=7):
    """Create log-scaled color breaks for mapping"""
    data_positive = data[data > 0]
    
    log_min = np.log10(data_positive.min())
    log_max = np.log10(data_positive.max())
    
    # Equal intervals in log space
    log_breaks = np.linspace(log_min, log_max, n_colors + 1)
    breaks = 10**log_breaks
    
    return breaks 

def create_leaflet_map(map_data, zoom=None, center=None):
    """Create a Leaflet map component with the hexagon data"""
    print("creating leaflet map")
    # print(json.loads(map_data.iloc[0]['hex_center'])['coordinates'])
    # Calculate center point from the data

    # center_lng, center_lat = json.loads(map_data.iloc[0]['hex_center'])['coordinates']
    # print(f"center_lng: {center_lng}, center_lat: {center_lat}")
    septiles = create_log_color_scale(map_data['count']) if len(map_data) > 0 else range(1, 8)
    print('septiles', [int(x) for x in septiles])
    legend = create_legend(septiles)

    hex_centers_lats = []
    hex_centers_lngs = []
    hex_boundaries_polygons = []
    hex_boundaries_geojson = {
        "type": "FeatureCollection",
        "features": []
    }
    dlPolygons = []
    for i in range(len(map_data)):
        # hex_centers_lats.append(json.loads(map_data.iloc[i]['hex_center'])['coordinates'][1])
        # hex_centers_lngs.append(json.loads(map_data.iloc[i]['hex_center'])['coordinates'][0])

        hex_boundaries_polygon = [[coord[1], coord[0]] for coord in json.loads(map_data.iloc[i]['hex_boundary'])['coordinates'][0]]
        hex_boundaries_polygons.append(hex_boundaries_polygon)
        
        hex_boundary_element = {'type': 'Feature'}
        hex_boundary_element['geometry'] = json.loads(map_data.iloc[i]['hex_boundary'])
        hex_centers_lats.append(json.loads(map_data.iloc[i]['hex_boundary'])['coordinates'][0][0][1])
        hex_centers_lngs.append(json.loads(map_data.iloc[i]['hex_boundary'])['coordinates'][0][0][0])
        hex_boundary_element['properties'] = {
            'count': map_data.iloc[i]['count'].item(),
            'color': style_function(map_data.iloc[i]['count'].item(), septiles)
        }
        hex_boundaries_geojson['features'].append(hex_boundary_element)

        style = style_function(map_data.iloc[i]['count'].item(), septiles)
        dlPolygons.append(
        {
            "positions": hex_boundaries_polygon,
            "fillColor": style['fillColor'],
            "color": style['color'],
            "weight": style['weight'],
            "opacity": style['opacity'],
            "fillOpacity": style['fillOpacity']
        })
        # print(dlPolygons)

    center_lat = sum(hex_centers_lats[:10]) / len(hex_centers_lats[:10]) if center is None else center['lat']
    center_lng = sum(hex_centers_lngs[:10]) / len(hex_centers_lngs[:10]) if center is None else center['lng']
    zoom = zoom if zoom is not None else 11
    print(f"center_lat: {center_lat}, center_lng: {center_lng}")

    # tile_layer_url = "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
    tile_layer_url = "http://a.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}.png"
    
    children = [
        dl.TileLayer(url=tile_layer_url, attribution='© Mapbox © OpenStreetMap')
    ]
    for dlP in dlPolygons:
        children.append(dl.Polygon(**dlP))
    
    map_component = dl.Map(
        children,
        trackViewport=True,
        center=[center_lat, center_lng], 
        zoom=zoom, 
        style={"width": "100%", "height": "100vh"},
        id="map-container"
    )
    
    # print("map_component", map_component)
    return map_component, legend

# Test database connection
# test_database_connection()

# Get initial data
print("getting data")
# catalog_list = get_catalogs()
default_catalog = 'mjohns'
default_schema = 'liquid_nyc_h3_trip'
default_table = 'h3_taxi_trips'
default_column = 'pickup_cell_12'
load_defaults = True
# schema_list = get_schemas(default_catalog)
# table_list = get_tables(default_catalog, default_schema)
# column_list = get_columns(default_catalog, default_schema, default_table)
# catalog_list = get_catalogs()

map_data = get_data(catalog=None, schema=None, table=None, column=None, bounds=None, resolution=8)
# map_data = get_data(catalog=default_catalog, schema=default_schema, table=default_table, column=default_column, bounds=None, resolution=8)
leaflet_map, legend = create_leaflet_map(map_data, zoom=11, center={'lat': 40.7128, 'lng': -74.0060})

# app.layout = dl.Map(dl.TileLayer(), center=[56, 10], zoom=6, style={"height": "50vh"})

app.layout = html.Div(
    [
        dcc.Location(id="url"),
        # Add dropdown selection controls at the top
        html.Div(
            [
                html.Div(
                    [
                        html.Label("Catalog:", style={"color": "#FFFFFF", "fontFamily": "Helvetica", "fontWeight": "bold", "marginRight": "10px"}),
                        dcc.Dropdown(
                            id="catalog-dropdown",
                            placeholder="Loading default...",
                            style={"width": "200px", "backgroundColor": "#FFFFFF", "fontFamily": "Helvetica", "color": "#3A3A3A"},
                            # options=catalog_list,
                            # value=default_catalog
                        )
                    ],
                    style={"display": "inline-block", "marginRight": "20px"}
                ),
                html.Div(
                    [
                        html.Label("Schema:", style={"color": "#FFFFFF", "fontFamily": "Helvetica","fontWeight": "bold", "marginRight": "10px"}),
                        dcc.Dropdown(
                            id="schema-dropdown",
                            placeholder="Loading default...",
                            disabled=True,
                            style={"width": "200px", "backgroundColor": "#FFFFFF", "fontFamily": "Helvetica", "color": "#3A3A3A"},
                        )
                    ],
                    style={"display": "inline-block", "marginRight": "20px"}
                ),
                html.Div(
                    [
                        html.Label("Table:", style={"color": "#FFFFFF", "fontFamily": "Helvetica", "fontWeight": "bold", "marginRight": "10px"}),
                        dcc.Dropdown(
                            id="table-dropdown",
                            placeholder="Loading default...",
                            disabled=True,
                            style={"width": "200px", "backgroundColor": "#FFFFFF", "fontFamily": "Helvetica", "color": "#3A3A3A"},
                        )
                    ],
                    style={"display": "inline-block", "marginRight": "20px"}
                ),
                html.Div(
                    [
                        html.Label("Column:", style={"color": "#FFFFFF", "fontFamily": "Helvetica", "fontWeight": "bold", "marginRight": "10px"}),
                        dcc.Dropdown(
                            id="column-dropdown",
                            placeholder="Loading default...",
                            disabled=True,
                            style={"width": "200px", "backgroundColor": "#FFFFFF", "fontFamily": "Helvetica", "color": "#3A3A3A"},
                        ),
                        # html.Div(
                        #     id="column-description",
                        #     style={
                        #         "color": "#FFFFFF", 
                        #         "fontFamily": "Helvetica", 
                        #         "fontSize": "12px",
                        #         "marginTop": "5px",
                        #         "minHeight": "20px"
                        #     }
                        # )
                    ],
                    style={"display": "inline-block", "marginRight": "20px"}
                ),
                html.Div(
                    [
                        html.P(
                            id="column-description",
                            style={
                                "color": "#FFFFFF", 
                                "fontFamily": "Helvetica", 
                                "fontSize": "14px",
                                "margin": "0",
                                "display": "inline-block",
                                "verticalAlign": "middle"
                            }
                        )
                    ],
                    style={"display": "inline-block", "marginRight": "20px"}
                ),
                html.Div(
                    [
                        dbc.Button(
                            "Refresh Data",
                            id="refresh-button",
                            className="refresh-button",
                            disabled=True,
                            style={
                                "backgroundColor": "#666666",
                                "color": "#CCCCCC",
                                "cursor": "not-allowed",
                                "borderRadius": "4px",
                                "fontSize": "14px",
                                "fontFamily": "Helvetica",
                                "fontWeight": "bold",
                                "width": "150px",
                                "height": "35px",
                                "border": "1px solid #999999",
                                "marginTop": "20px",
                                "opacity": "0.6"
                            }
                        )
                    ],
                    style={"display": "inline-block", "marginRight": "0px", "float": "right"}
                ),
            ],
            style={
                "backgroundColor": "#3A3A3A",
                "padding": "15px",
                "borderRadius": "8px",
                # "marginBottom": "20px",
                "boxShadow": "0 2px 4px rgba(0,0,0,0.3)"
            }
        ),
        # Add refresh button at the top right
        # html.Div(
        #     dbc.Button(
        #         "Refresh Data",
        #         id="refresh-button2",
        #         className="mb-3",
        #         style={
        #             "backgroundColor": "#3A3A3A",
        #             "color": "#FFFFFF",
        #             "cursor": "pointer",
        #             "borderRadius": "4px",
        #             "fontSize": "14px",
        #             "fontFamily": "Helvetica",
        #             "fontWeight": "bold",
        #             "position": "absolute",
        #             "top": "10px",
        #             "right": "10px",
        #             "zIndex": "1000",
        #             "padding": "8px 16px"
        #         }
        #     ),
        #     style={"position": "relative"}
        # ),
        # # Add legend with id for callback
        # html.Div(id="legend-container"),
        # # Add map with id for callback
        # html.Div(id="map-container")
        dcc.Loading(
            children=[
                html.Div(id="map-container", children=leaflet_map),
                html.Div(id="legend-container", children=legend)
            ],
            id='loading-map',
            type='default',
            color='#FFFFFF',
            overlay_style={"visibility":"visible", "filter": "blur(3px)"},
        ),
        # # Hidden interval component
        # dcc.Interval(
        #     id="on-load-trigger",
        #     interval=1*1000,  # 1 second (small delay to ensure page load)
        #     n_intervals=0,
        #     max_intervals=1   # only fire once
        # ),
    ],
    style={"backgroundColor": "#29323C"},
    
)

# Add callback for refresh button and initial load
@app.callback(
    [Output('map-container', 'children'),
     Output('legend-container', 'children')],
    Input('refresh-button', 'n_clicks'),
    [State("map-container", "center"),
     State("map-container", "zoom"),
     State("map-container", "bounds"),
     State("catalog-dropdown", "value"),
     State("schema-dropdown", "value"),
     State("table-dropdown", "value"),
     State("column-dropdown", "value"),
     State("column-description", "children")
     ],
     prevent_initial_call=True
)
def update_map_and_legend(n_clicks, center, zoom, bounds, catalog, schema, table, column, column_description):
    if n_clicks is None:
        # Initial load - return the pre-created map and legend
        print("Initial map load")
        return leaflet_map, legend
    else:
        # Refresh button clicked
        print(f"Refreshing map and data (click #{n_clicks})")

        # Update global variables
        global global_center
        global global_zoom
        global global_bounds
        global_center = center if center is not None else global_center
        global_zoom = zoom if zoom is not None else global_zoom
        global_bounds = bounds if bounds is not None else global_bounds

        column_resolution = column_description.split(";")[0].split(":")[1].strip()
        column_count = column_description.split(";")[1].split(":")[1].strip()
        print(f"Center: {center}, Zoom: {zoom}, Bounds: {bounds}, Column Resolution: {column_resolution}, Column Count: {column_count}")
        if isinstance(global_center, list):
            global_center = {'lat': global_center[0], 'lng': global_center[1]}
        print(f"Global Center: {global_center}, Global Zoom: {global_zoom}, Global Bounds: {global_bounds}")

        resolution = zoom_to_h3_resolution(global_zoom)

        # Fetch new data
        new_map_data = get_data(catalog=catalog, schema=schema, table=table, column=column, bounds=global_bounds, resolution=resolution, column_resolution=column_resolution)
        
        # Create new map and legend
        new_leaflet_map, new_legend = create_leaflet_map(new_map_data, zoom=global_zoom, center=global_center)
        
        print("Map refreshed successfully!")
        return new_leaflet_map, new_legend

# @app.callback(
#     # Output("viewport-info", "children"),
#     [Inp]
#     [State("map-container", "center"),
#      State("map-container", "zoom"),
#      State("map-container", "bounds")]  # This gets updated when viewport changes
# )
# def update_viewport_info(center, zoom, bounds):
#     if center is None:
#         return "Viewport not yet initialized"
    
#     print(f"Center: {center}, Zoom: {zoom}, Bounds: {bounds}")

#     return f"Center: {center}, Zoom: {zoom}, Bounds: {bounds}"

# Callback to populate catalog dropdown on app load
@app.callback(
    [Output('catalog-dropdown', 'options'),
     Output('catalog-dropdown', 'placeholder'),
     Output('catalog-dropdown', 'disabled'),
     Output('catalog-dropdown', 'value')],
    [Input('catalog-dropdown', 'id'),
     Input("url", "pathname")],
    prevent_initial_call=False
)
def populate_catalogs(trigger, pathname):
    """Populate the catalog dropdown with available catalogs"""
    print("Populating catalogs dropdown...")

    global load_defaults
    global default_catalog
    
    try:
        print("Fetching catalogs from database...")
        catalogs = get_catalogs()
        # print(f"Retrieved catalogs: {catalogs}")
        print("returning:", default_catalog)
        return catalogs, "Select a catalog...", False, default_catalog if load_defaults else None
        
    except Exception as e:
        print(f"Error fetching catalogs: {e}")
        print("Falling back to mock data")
        return [{'label': 'mock_catalog', 'value': 'mock_catalog'}]

# Callback to populate schema dropdown when catalog is selected
@app.callback(
    [Output('schema-dropdown', 'options'),
     Output('schema-dropdown', 'placeholder'),
     Output('schema-dropdown', 'disabled'),
     Output('schema-dropdown', 'value')],
    [Input('catalog-dropdown', 'value')],
    prevent_initial_call=True
)
def populate_schemas(selected_catalog):
    """Populate the schema dropdown when a catalog is selected"""
    print(f"Populating schemas for catalog: {selected_catalog}")

    global load_defaults
    global default_schema

    if not selected_catalog:
        return [], "Select a schema...", True, None
    
    # if USE_MOCK_DATA:
    #     # Return mock schemas for development
    #     print("Using mock data for schemas")
    #     return [{'label': 'PERMISSION DENIED', 'value': 'PERMISSION DENIED'}], "Select a schema...", False, None
    
    try:
        print(f"Fetching schemas for catalog {selected_catalog}...")
        schemas = get_schemas(selected_catalog)
        print(f"IN SCHEMAS: default_catalog: {default_catalog}, load_defaults: {load_defaults}")
        return schemas, "Select a schema...", False, default_schema if load_defaults else None
    
    except Exception as e:
        print(f"Error fetching schemas for catalog {selected_catalog}: {e}")
        print("Falling back to mock data")
        return [{'label': 'PERMISSION DENIED', 'value': 'PERMISSION DENIED'}], "Select a schema...", False, None

# Callback to populate table dropdown when schema is selected
@app.callback(
    [Output('table-dropdown', 'options'),
     Output('table-dropdown', 'placeholder'),
     Output('table-dropdown', 'disabled'),
     Output('table-dropdown', 'value')],
    Input('schema-dropdown', 'value'),
    State('catalog-dropdown', 'value'),
    prevent_initial_call=False
)
def populate_tables(selected_schema, selected_catalog):
    """Populate the table dropdown when a schema is selected"""
    print(f"Populating tables for schema: {selected_schema}, catalog: {selected_catalog}")
    
    global load_defaults
    global default_table

    if not selected_schema or not selected_catalog:
        return [], "Select a table...", True, None
    
    try:
        print(f"Fetching tables for schema {selected_catalog}.{selected_schema}...")
        tables = get_tables(selected_catalog, selected_schema)
        print(f"Retrieved tables: {tables}")
        return tables, "Select a table...", False, default_table if load_defaults else None
    except Exception as e:
        print(f"Error fetching tables for schema {selected_catalog}.{selected_schema}: {e}")
        print("ASSUMING PERMISSION DENIED")
        return [{'label': 'PERMISSION DENIED', 'value': 'PERMISSION DENIED'}], "Select a table...", False, None

# Callback to populate column dropdown when table is selected
@app.callback(
    [Output('column-dropdown', 'options'),
     Output('column-dropdown', 'placeholder'),
     Output('column-dropdown', 'disabled'),
     Output('column-dropdown', 'value')],
    Input('table-dropdown', 'value'),
    [State('catalog-dropdown', 'value'),
     State('schema-dropdown', 'value')],
    prevent_initial_call=False
)
def populate_columns(selected_table, selected_catalog, selected_schema):
    """Populate the column dropdown when a table is selected"""
    print(f"Populating columns for table: {selected_table}, schema: {selected_schema}, catalog: {selected_catalog}")

    global load_defaults
    global default_column

    if not selected_table or not selected_catalog or not selected_schema:
        return [], "Select a column...", True, None
    
    try:
        print(f"Fetching columns for table {selected_catalog}.{selected_schema}.{selected_table}...")
        columns = get_columns(selected_catalog, selected_schema, selected_table)
        print(f"Retrieved columns: {columns}")
        return columns, "Select a column...", False, default_column if load_defaults else None
    except Exception as e:
        print(f"Error fetching columns for table {selected_catalog}.{selected_schema}.{selected_table}: {e}")
        print("ASSUMING PERMISSION DENIED")
        return ['PERMISSION DENIED'], "Select a column...", False, None

# Callback to validate column and show description
@app.callback(
    [Output('column-description', 'children'),
     Output('refresh-button', 'disabled'),
     Output('refresh-button', 'style')],
    Input('column-dropdown', 'value'),
    [State('catalog-dropdown', 'value'),
     State('schema-dropdown', 'value'),
     State('table-dropdown', 'value')],
    prevent_initial_call=False
)
def validate_column(selected_column, selected_catalog, selected_schema, selected_table):
    """Validate the selected column and return description"""
    disabled_style = {
        "backgroundColor": "#666666",
        "color": "#CCCCCC",
        "cursor": "not-allowed",
        "borderRadius": "4px",
        "fontSize": "14px",
        "fontFamily": "Helvetica",
        "fontWeight": "bold",
        "width": "150px",
        "height": "35px",
        "border": "1px solid #999999",
        "marginTop": "20px",
        "opacity": "0.6"
    }
    enabled_style = {
        "backgroundColor": "#3A3A3A",
        "color": "#FFFFFF",
        "cursor": "pointer",
        "borderRadius": "4px",
        "fontSize": "14px",
        "fontFamily": "Helvetica",
        "fontWeight": "bold",
        "width": "150px",
        "height": "35px",
        "border": "1px solid #FFFFFF",
        "marginTop": "20px"
    }
    
    if not selected_column or not selected_catalog or not selected_schema or not selected_table:
        # disabled_style = {
        #     "backgroundColor": "#666666",
        #     "color": "#CCCCCC",
        #     "cursor": "not-allowed",
        #     "borderRadius": "4px",
        #     "fontSize": "14px",
        #     "fontFamily": "Helvetica",
        #     "fontWeight": "bold",
        #     "width": "150px",
        #     "height": "35px",
        #     "border": "1px solid #999999",
        #     "marginTop": "20px",
        #     "opacity": "0.6"
        # }
        return "", True, disabled_style
    
    print(f"Validating column: {selected_column}, table: {selected_table}, schema: {selected_schema}, catalog: {selected_catalog}")

    try:
        resolution_query = f"SELECT h3_resolution({selected_column}) as resolution FROM {selected_catalog}.{selected_schema}.{selected_table} LIMIT 1"
        column_resolution = sqlQuery(resolution_query)['resolution'].iloc[0]
        print(f"Column resolution: {column_resolution}")

        count_query = f"SELECT COUNT(*) as count FROM {selected_catalog}.{selected_schema}.{selected_table} WHERE {selected_column} IS NOT NULL"
        count_result = sqlQuery(count_query)['count'].iloc[0]
        print(f"Count result: {count_result}")

        if column_resolution is None or column_resolution == 0 or count_result == 0:
            print("Column resolution is None or count is 0.")
            # disabled_style = {
            #     "backgroundColor": "#666666",
            #     "color": "#CCCCCC",
            #     "cursor": "not-allowed",
            #     "borderRadius": "4px",
            #     "fontSize": "14px",
            #     "fontFamily": "Helvetica",
            #     "fontWeight": "bold",
            #     "width": "150px",
            #     "height": "35px",
            #     "border": "1px solid #999999",
            #     "marginTop": "20px",
            #     "opacity": "0.6"
            # }
            return "Column is not valid H3", True, disabled_style
        else:
            print("Column resolution is valid. Returning columns.")
            # enabled_style = {
            #     "backgroundColor": "#3A3A3A",
            #     "color": "#FFFFFF",
            #     "cursor": "pointer",
            #     "borderRadius": "4px",
            #     "fontSize": "14px",
            #     "fontFamily": "Helvetica",
            #     "fontWeight": "bold",
            #     "width": "150px",
            #     "height": "35px",
            #     "border": "1px solid #FFFFFF",
            #     "marginTop": "20px"
            # }
            return f"Column resolution: {column_resolution}; Row count: {format(count_result, ',')}", False, enabled_style
    except Exception as e:
        print(f"Column is not valid H3: {e}")
        # disabled_style = {
        #     "backgroundColor": "#666666",
        #     "color": "#CCCCCC",
        #     "cursor": "not-allowed",
        #     "borderRadius": "4px",
        #     "fontSize": "14px",
        #     "fontFamily": "Helvetica",
        #     "fontWeight": "bold",
        #     "width": "150px",
        #     "height": "35px",
        #     "border": "1px solid #999999",
        #     "marginTop": "20px",
        #     "opacity": "0.6"
        # }
        return "Column is not valid H3", True, disabled_style

# # Callback to reset dependent dropdowns when parent selection changes
# @app.callback(
#     [Output('schema-dropdown', 'value'),
#      Output('table-dropdown', 'value'),
#      Output('column-dropdown', 'value')],
#     [Input('catalog-dropdown', 'value'),
#      Input('schema-dropdown', 'value'),
#      Input('table-dropdown', 'value')],
#     prevent_initial_call=True
# )
# def reset_dependent_dropdowns(catalog_value, schema_value, table_value):
#     """Reset dependent dropdowns when parent selection changes"""
#     ctx = callback_context
#     if not ctx.triggered:
#         return None, None, None
    
#     triggered_id = ctx.triggered[0]['prop_id'].split('.')[0]
    
#     if triggered_id == 'catalog-dropdown':
#         # Reset schema, table, and column when catalog changes
#         return None, None, None
#     elif triggered_id == 'schema-dropdown':
#         # Reset table and column when schema changes
#         return no_update, None, None
#     elif triggered_id == 'table-dropdown':
#         # Reset column when table changes
#         return no_update, no_update, None
    
#     return no_update, no_update, no_update


if __name__ == "__main__":
    app.run(debug=True)