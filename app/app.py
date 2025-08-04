import os
from databricks import sql
import pandas as pd
import numpy as np
from shapely.geometry import Polygon
import dash
from dash import dcc, html, Input, Output, State, callback_context
import plotly.express as px
import dash_bootstrap_components as dbc
import dash_ag_grid as dag
from databricks.sdk.core import Config
import dash_leaflet as dl
from dash.dependencies import Input, Output
from dash_extensions.javascript import arrow_function
import json
import h3
import datetime as dt
import time

# Set up the app
app = dash.Dash(__name__)

# Check for environment variables but don't fail if they're not set (for development)
DATABRICKS_WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

global_center = None
global_zoom = None
global_bounds = None

if not DATABRICKS_WAREHOUSE_ID or not DATABRICKS_TOKEN:
    print("Warning: DATABRICKS_WAREHOUSE_ID or DATABRICKS_TOKEN not set. Using mock data for development.")
    USE_MOCK_DATA = True
else:
    USE_MOCK_DATA = False

def sqlQuery(query: str) -> pd.DataFrame:
    """Execute a SQL query and return the result as a pandas DataFrame."""
    with sql.connect(
        http_path=f"/sql/1.0/warehouses/{DATABRICKS_WAREHOUSE_ID}",
        server_hostname="e2-demo-field-eng.cloud.databricks.com",
        access_token=DATABRICKS_TOKEN
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            df = pd.DataFrame(rows, columns=columns)
        return df

def get_mock_data():
    """Generate mock data for development when database is not available"""
    print("Generating mock data for development...")
    
    # Create some mock hexagons around NYC
    mock_hexagons = [
        "85283473fffffff",  # NYC area H3 hexagon
        "85283477fffffff",
        "8528347bfffffff",
        "85283483fffffff",
        "85283487fffffff",
        "8528348bfffffff",
        "8528348ffffffff",
        "85283493fffffff",
        "85283497fffffff"
    ]
    
    mock_data = []
    for i, hex_id in enumerate(mock_hexagons):
        # Generate mock boundary and center
        try:
            hex_boundary = h3.cell_to_boundary(hex_id)
            hex_center = h3.cell_to_latlng(hex_id)
            
            # Convert boundary to GeoJSON format
            boundary_coords = [[coord[1], coord[0]] for coord in hex_boundary]  # Convert to [lng, lat]
            
            mock_data.append({
                'hex_id': hex_id,
                'hex_boundary': json.dumps({
                    "type": "Polygon",
                    "coordinates": [boundary_coords]
                }),
                'hex_center': json.dumps({
                    "type": "Point",
                    "coordinates": [hex_center[1], hex_center[0]]  # GeoJSON format: [lng, lat]
                }),
                'count': np.random.randint(10, 500)  # Random count for demo
            })
        except Exception as e:
            print(f"Error generating mock data for hex {hex_id}: {e}")
            continue
    
    return pd.DataFrame(mock_data)

# Fetch the all h3 data
def get_data(resolution=9, bounds=None):
    stime = dt.datetime.now()
    
    print(f"USE_MOCK_DATA: {USE_MOCK_DATA}")
    if USE_MOCK_DATA:
        data = get_mock_data()
    else:
        try:
            bounds_wkt = bounds_to_wkt(bounds) if bounds else None
            # query = f"""WITH cell_agg AS (
            #                         SELECT
            #                             h3_toparent(pickup_cell_12, {resolution}) as h3_cell_id,
            #                             count(*) as count
            #                         FROM mjohns.liquid_nyc_h3_trip.h3_taxi_trips
            #                         GROUP BY h3_cell_id
            #                         )
            #                     SELECT h3_boundaryasgeojson(h3_cell_id) as hex_boundary,
            #                          count
            #                     FROM cell_agg
            #                     WHERE {f"ST_CONTAINS(ST_GEOMFROMWKT('{bounds_wkt}'), ST_GEOMFROMWKT(H3_CENTERASWKT(h3_cell_id)))" if bounds_wkt else "1=1"}
            #                     ORDER BY count DESC"""
            
            # query = f"""WITH cell_agg AS (
            #                         SELECT
            #                             h3_toparent(pickup_cell_12, {resolution}) as h3_cell_id,
            #                             count(*) as count
            #                         FROM mjohns.liquid_nyc_h3_trip.h3_taxi_trips
            #                         GROUP BY h3_cell_id
            #                         )
            #                     SELECT h3_boundaryasgeojson(h3_cell_id) as hex_boundary,
            #                          count
            #                     FROM cell_agg
            #                     WHERE {f"ARRAY_CONTAINS(H3_COVERASH3('{bounds_wkt}', {resolution}), h3_cell_id)" if bounds_wkt else "1=1"}
            #                     ORDER BY count DESC"""
            query = f"""
                        WITH cell_agg AS (
                        SELECT
                            h3_toparent(pickup_cell_12, {resolution}) as h3_cell_id,
                            count(*) as count
                        FROM mjohns.liquid_nyc_h3_trip.h3_taxi_trips
                        WHERE {f"h3_toparent(pickup_cell_12, {resolution}) IN (SELECT EXPLODE(H3_COVERASH3('{bounds_wkt}', {resolution})))" if bounds_wkt else "1=1"}
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
            data = get_mock_data()
    
    print(f"DATA QUERY TOOK:    {dt.datetime.now() - stime}")
    print(data.head())
    print(len(data), "rows")
    return data

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
        "top": "60px",
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
    septiles = create_log_color_scale(map_data['count'])
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

# Get initial data
print("getting data")
map_data = get_data(bounds=None, resolution=8)
# geojson_data = create_hexagon_geojson(map_data)
leaflet_map, legend = create_leaflet_map(map_data, zoom=11, center={'lat': 40.7128, 'lng': -74.0060})

# app.layout = dl.Map(dl.TileLayer(), center=[56, 10], zoom=6, style={"height": "50vh"})

app.layout = html.Div(
    [
        # Add refresh button at the top right
        html.Div(
            dbc.Button(
                "Refresh Data",
                id="refresh-button",
                className="mb-3",
                style={
                    "backgroundColor": "#3A3A3A",
                    "color": "#FFFFFF",
                    "cursor": "pointer",
                    "borderRadius": "4px",
                    "fontSize": "14px",
                    "fontFamily": "Helvetica",
                    "fontWeight": "bold",
                    "position": "absolute",
                    "top": "10px",
                    "right": "10px",
                    "zIndex": "1000",
                    "padding": "8px 16px"
                }
            ),
            style={"position": "relative"}
        ),
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
    ],
    style={"backgroundColor": "#29323C"}
)

# Add callback for refresh button and initial load
@app.callback(
    [Output('map-container', 'children'),
     Output('legend-container', 'children')],
    Input('refresh-button', 'n_clicks'),
    [State("map-container", "center"),
     State("map-container", "zoom"),
     State("map-container", "bounds")],
     prevent_initial_call=True
)
def update_map_and_legend(n_clicks, center, zoom, bounds):
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

        print(f"Center: {center}, Zoom: {zoom}, Bounds: {bounds}")
        if isinstance(global_center, list):
            global_center = {'lat': global_center[0], 'lng': global_center[1]}
        print(f"Global Center: {global_center}, Global Zoom: {global_zoom}, Global Bounds: {global_bounds}")

        resolution = zoom_to_h3_resolution(global_zoom)

        # Fetch new data
        new_map_data = get_data(bounds=global_bounds, resolution=resolution)
        
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


if __name__ == "__main__":
    app.run(debug=True)