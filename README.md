# Geospatial Visualization App with Leaflet

A modern, interactive web application for visualizing massive H3 geospatial data on Databricks using Leaflet maps. This application provides an intuitive interface for exploring and analyzing geospatial datasets with dynamic zoom levels and real-time data filtering.

## 🌟 Features

- **Interactive Leaflet Maps**: Beautiful, responsive maps with dark theme styling
- **H3 Hexagon Visualization**: Efficient rendering of H3 geospatial data at multiple resolutions
- **Dynamic Data Loading**: Real-time data fetching based on map viewport and zoom level
- **Multi-level Data Selection**: Hierarchical dropdowns for catalog, schema, table, and column selection
- **Adaptive Resolution**: Automatic H3 resolution adjustment based on zoom level for optimal performance
- **Color-coded Heatmaps**: Logarithmic color scaling for better data representation
- **Responsive UI**: Modern, clean interface with Bootstrap components
- **Databricks Integration**: Seamless connection to Databricks SQL warehouses

## 🏗️ Architecture

The application is built using:
- **Frontend**: Dash (Python web framework) with Bootstrap components
- **Maps**: Leaflet.js for interactive mapping
- **Data Processing**: Pandas and NumPy for data manipulation
- **Geospatial**: H3 library for hexagonal grid operations
- **Database**: Databricks SQL connector for data access
- **Deployment**: Databricks Asset Bundles for easy deployment

## 📋 Prerequisites

- Python 3.8+
- Databricks workspace access
- SQL warehouse permissions
- H3 geospatial data in your Databricks tables

## 🚀 Installation

### Local Development

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd geospatial_viz_app_leaflet
   ```

2. **Install dependencies**:
   ```bash
   cd app
   pip install -r requirements.txt
   ```

3. **Set environment variables for app**:
   ```bash
   export DATABRICKS_WAREHOUSE_ID="your_warehouse_id"
   export DATABRICKS_HOST="your-workspace.cloud.databricks.com"
   export DATABRICKS_TOKEN="your_access_token"*
   export DATABRICKS_BUDGET_POLICY_ID="your_budget_policy_id"
   export DEFAULT_CATALOG="your_catalog"
   export DEFAULT_SCHEMA="your_schema"
   export DEFAULT_TABLE="your_table"
   export DEFAULT_COLUMN="your_h3_column"
   ```

3. **Set environment variables for bundle deployment (DAB)**:
   ```bash
   export BUNDLE_VAR_sql_warehouse_id=$DATABRICKS_WAREHOUSE_ID
   export BUNDLE_VAR_budget_policy_id=$DATABRICKS_BUDGET_POLICY_ID
   ```

4. **Run the application**:
   ```bash
   python app.py
   ```

### Databricks Deployment

1. **Configure your Databricks workspace** in `databricks.yml`
2. **Deploy using Databricks Asset Bundles**:
   ```bash
   databricks bundle deploy
   ```

## 🔧 Configuration

### Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `DATABRICKS_WAREHOUSE_ID` | SQL warehouse ID for data access | Yes |
| `DATABRICKS_HOST` | Databricks workspace URL | Yes |
| `DATABRICKS_TOKEN` | Access token for authentication | Yes* |
| `DATABRICKS_BUDGET_POLICY_ID` | Databricks budget policy id | No |
| `DEFAULT_CATALOG` | Default catalog to load | No |
| `DEFAULT_SCHEMA` | Default schema to load | No |
| `DEFAULT_TABLE` | Default table to load | No |
| `DEFAULT_COLUMN` | Default H3 column to visualize | No |

*Can use on-behalf-of authentication if not set

### Data Requirements

Your Databricks tables should contain:
- **H3 columns**: Hexagonal grid identifiers at various resolutions
- **Geospatial data**: Location-based information for visualization
- **Count/aggregation columns**: Numerical data for color coding

## 📊 Usage

### Basic Operation

1. **Launch the application** and wait for initial data load
2. **Select data source** using the dropdown controls:
   - Catalog → Schema → Table → Column
3. **Navigate the map** using standard Leaflet controls
4. **Refresh data** using the refresh button to update based on current viewport

### Map Controls

- **Zoom**: Mouse wheel or zoom controls
- **Pan**: Click and drag to move around
- **Resolution**: Automatically adjusts H3 resolution based on zoom level
- **Bounds**: Data is filtered based on current map viewport

### Data Visualization

- **Color Scale**: Logarithmic scaling for better data distribution
- **Legend**: Interactive legend showing count ranges and colors
- **Performance**: Viewport-based data loading for large datasets

## 🗺️ H3 Resolution Mapping

The application automatically adjusts H3 resolution based on zoom level:

| Zoom Level | H3 Resolution | Use Case |
|------------|---------------|----------|
| 4-7 | 5-6 | Country/region view |
| 8-11 | 7-9 | City/metropolitan view |
| 12-15 | 10-11 | Neighborhood view |
| 16+ | 12 | Street-level detail |

## 🔌 API Endpoints

The application provides several internal endpoints for data management:

- **Data Fetching**: Dynamic SQL queries based on viewport and resolution
- **Metadata Discovery**: Automatic catalog, schema, table, and column discovery
- **H3 Validation**: Column validation for H3 geospatial data
- **Performance Optimization**: Viewport-based data loading

## 🚀 Performance Features

- **Lazy Loading**: Data is only fetched when needed
- **Viewport Filtering**: Queries are limited to visible map area
- **Resolution Optimization**: H3 resolution automatically adjusts for performance
- **Connection Pooling**: Efficient Databricks SQL connections

## 🛠️ Development

### Project Structure

```
geospatial_viz_app_leaflet/
├── app/
│   ├── app.py              # Main application file
│   ├── app.yml             # App configuration
│   └── requirements.txt    # Python dependencies
├── resources/
│   └── geospatial_viz_app_leaflet.yml  # Databricks resources
├── databricks.yml          # Bundle configuration
└── README.md               # This file
```

### Key Components

- **`app.py`**: Main Dash application with all callbacks and UI logic
- **Map Creation**: `create_leaflet_map()` function for map generation
- **Data Fetching**: `get_data()` function for SQL queries
- **H3 Processing**: Resolution mapping and boundary conversion
- **UI Components**: Dropdowns, buttons, and responsive layout

### Adding New Features

1. **New Map Layers**: Extend the `create_leaflet_map()` function
2. **Additional Data Sources**: Modify the `get_data()` function
3. **UI Enhancements**: Add new Dash components to the layout
4. **Styling**: Update CSS and Bootstrap classes

## 🧪 Testing

### Local Testing

```bash
cd app
python app.py
```

### Databricks Testing

```bash
databricks bundle validate
databricks bundle deploy --target dev
```


---

**Built with ❤️ using Dash, Leaflet, and Databricks**
