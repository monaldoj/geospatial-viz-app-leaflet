# Massive Scale H3 Visualization with Leaflet on Databricks-Apps

A interactive web application for visualizing massive H3 geospatial data on Databricks using Leaflet maps. This application demostrates the speed of exploring and visualizing any table in your Databricks workspace, which has an H3 index column. The responsiveness of the app is due using dynamic zoom levels determine the H3 resolution and refreshing the map to filter on the viewport to only pull and visualize relevant data.

![Scaling Geospatial Analytics Demo](images/scaling_geospatial_analytics_blog.gif)

## 🌟 Features

- **Interactive Leaflet Maps**: Responsive maps with dark theme styling
- **H3 Hexagon Visualization**: Efficient rendering of H3 geospatial data at multiple resolutions
- **Dynamic Data Loading**: Real-time data fetching based on map viewport and zoom level
- **Multi-level Data Selection**: Select your catalog, schema, table, and column selection to visualize any table in your Databricks workspace.
- **Adaptive Resolution**: Automatic H3 resolution adjustment based on zoom level for optimal performance
- **Color-coded Heatmaps**: Logarithmic color scaling for better data representation
- **Databricks Integration**: Seamless connection to Databricks SQL warehouses for backend aggregations
- **Databricks Asset Bundles (DABS)**: Built using DABs to efficiently deploy to your workspace

## 🏗️ Architecture

The application is built using:
- **Databricks Apps**: Deploy the app directly in your Databricks workspace
- **On-behalf-on authentication**: Only displays data which app users have access to in Unity Catalog
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
   export DATABRICKS_BUDGET_POLICY_ID="your_budget_policy_id"**
   export DEFAULT_CATALOG="your_catalog"
   export DEFAULT_SCHEMA="your_schema"
   export DEFAULT_TABLE="your_table"
   export DEFAULT_COLUMN="your_h3_column"
   ```

    *Can use on-behalf-of authentication if not set, but local development requires a token
    **Budget policy is option when deploying the DAB

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
2. **Validate your Databricks Asset Bundle**:
   ```bash
   databricks bundle validate
   ```
3. **Deploy using Databricks Asset Bundles**:
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

Your Databricks table should contain:
- **H3 columns**: Valid H3 index column

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

## 🚀 Performance Features

- **Lazy Loading**: Data is only fetched when needed
- **Viewport Filtering**: Queries are limited to visible map area
- **Resolution Optimization**: H3 resolution automatically adjusts for performance
- **Server-side Processing**: Performs aggregations in Databricks SQL warehouses

## 🛠️ Development

### Project Structure

```
geospatial_viz_app_leaflet/
├── app/
│   ├── app.py              # Main application file
│   ├── app.yml             # App configuration
│   └── requirements.txt    # Python dependencies
├── notebooks/
│   ├── nb01-download-prep-large-scale-dataset.dbc      # Databricks notebook for data preparation
│   ├── nb01-download-prep-large-scale-dataset.ipynb    # Jupyter notebook for data preparation
│   ├── nb02-query-explore-large-scale-tables.dbc       # Databricks notebook for data exploration
│   └── nb02-query-explore-large-scale-tables.ipynb     # Jupyter notebook for data exploration
├── resources/
│   └── geospatial_viz_app_leaflet.yml  # Databricks resources
├── databricks.yml          # Bundle configuration
└── README.md               # This file
```

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

**Built using Databricks, Dash, and Leaflet**
