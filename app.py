"""
Parquet Viewer - A Streamlit application for viewing and exploring Parquet files.
Optimized for large files with millions of rows using lazy loading.

Features:
- Upload and view Parquet files
- Display schema information
- Display metadata
- Filter and sort data
- Paginate through large datasets
"""

import streamlit as st
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import io


# Page configuration
st.set_page_config(
    page_title="Parquet Viewer",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="expanded"
)

# Default constants for performance
DEFAULT_MAX_ROWS_FULL_LOAD = 100000  # Only load all data if less than this
DEFAULT_PAGE_SIZE = 25


def initialize_session_state():
    """Initialize session state variables if they don't exist."""
    defaults = {
        "uploaded_file": None,
        "parquet_file": None,
        "total_rows": 0,
        "num_row_groups": 0,
        "columns": [],
        "filter_column": None,
        "filter_value": "",
        "page_size": DEFAULT_PAGE_SIZE,
        "current_page": 1,
        "selected_columns": None,  # For column pruning
        "max_rows_full_load": DEFAULT_MAX_ROWS_FULL_LOAD,
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def load_parquet_metadata(uploaded_file) -> tuple[pq.ParquetFile, int, int, list] | tuple[None, None, None, None]:
    """
    Load only metadata from parquet file without reading all data.
    
    Args:
        uploaded_file: The file object from st.file_uploader
        
    Returns:
        Tuple of (ParquetFile, total_rows, num_row_groups, columns) or Nones if error
    """
    try:
        bytes_data = uploaded_file.getvalue()
        parquet_file = pq.ParquetFile(io.BytesIO(bytes_data))
        metadata = parquet_file.metadata
        
        total_rows = metadata.num_rows
        num_row_groups = metadata.num_row_groups
        # Use schema_arrow.fields which returns a list of fields
        columns = [field.name for field in parquet_file.schema_arrow]
        
        return parquet_file, total_rows, num_row_groups, columns
    except Exception as e:
        st.error(f"Error loading parquet file: {str(e)}")
        return None, None, None, None


def read_page_efficiently(parquet_file: pq.ParquetFile, page: int, page_size: int, 
                          columns: list | None = None) -> pd.DataFrame:
    """
    Read only the rows needed for the current page using row group filtering.
    For large files, this avoids loading entire dataset into memory.
    
    Args:
        parquet_file: PyArrow ParquetFile object
        page: Current page number (1-indexed)
        page_size: Number of rows per page
        columns: List of columns to read (None = all columns)
        
    Returns:
        DataFrame with paginated data
    """
    try:
        total_rows = parquet_file.metadata.num_rows
        start_row = (page - 1) * page_size
        end_row = min(start_row + page_size, total_rows)
        
        # For small files or if we need all columns, read specific row range
        if total_rows <= st.session_state.max_rows_full_load:
            # Read all data and slice (efficient for small files)
            table = parquet_file.read(columns=columns)
            df = table.to_pandas()
            return df.iloc[start_row:end_row]
        else:
            # For large files, use row group based reading
            # Find which row groups contain our target rows
            row_group_offsets = []
            current_offset = 0
            
            for rg_idx in range(parquet_file.metadata.num_row_groups):
                rg_metadata = parquet_file.metadata.row_group(rg_idx)
                num_rows = rg_metadata.num_rows
                row_group_offsets.append((current_offset, current_offset + num_rows, rg_idx))
                current_offset += num_rows
            
            # Collect row groups that overlap with our page
            chunks = []
            for start, end, rg_idx in row_group_offsets:
                if end > start_row and start < end_row:
                    # This row group contains rows we need
                    table = parquet_file.read_row_group(rg_idx, columns=columns)
                    chunks.append(table.to_pandas())
            
            if chunks:
                df = pd.concat(chunks, ignore_index=True)
                # Adjust slice to get exact page rows
                page_start = start_row - row_group_offsets[0][0] if row_group_offsets else 0
                return df.iloc[page_start:page_start + page_size]
            else:
                return pd.DataFrame()
    except Exception as e:
        st.error(f"Error reading page: {str(e)}")
        return pd.DataFrame()


def get_schema_dataframe(parquet_file: pq.ParquetFile) -> pd.DataFrame:
    """
    Extract schema information from a ParquetFile.
    
    Args:
        parquet_file: PyArrow ParquetFile object
        
    Returns:
        DataFrame with columns: Column Name, Data Type, Nullable
    """
    schema = parquet_file.schema_arrow
    schema_data = []
    
    for field in schema:
        schema_data.append({
            "Column Name": field.name,
            "Data Type": str(field.type),
            "Nullable": "Yes" if field.nullable else "No"
        })
    
    return pd.DataFrame(schema_data)


def get_metadata_dict(parquet_file: pq.ParquetFile) -> dict:
    """
    Extract metadata from a ParquetFile.
    
    Args:
        parquet_file: PyArrow ParquetFile object
        
    Returns:
        Dictionary of metadata properties
    """
    metadata = parquet_file.metadata
    
    # Calculate approximate memory usage
    total_bytes = metadata.serialized_size
    for rg_idx in range(metadata.num_row_groups):
        rg = metadata.row_group(rg_idx)
        total_bytes += rg.total_byte_size
    
    size_mb = total_bytes / (1024 * 1024)
    
    return {
        "Format Version": metadata.format_version,
        "Created By": metadata.created_by or "Unknown",
        "Number of Rows": f"{metadata.num_rows:,}",
        "Number of Row Groups": metadata.num_row_groups,
        "Serialized Size": f"{metadata.serialized_size:,} bytes",
        "Approximate Size": f"{size_mb:.2f} MB",
    }


def paginate_dataframe(df: pd.DataFrame, page: int, page_size: int) -> tuple[pd.DataFrame, int]:
    """
    Paginate a DataFrame.
    
    Args:
        df: Input DataFrame
        page: Current page number (1-indexed)
        page_size: Number of rows per page
        
    Returns:
        Tuple of (paginated DataFrame, total pages)
    """
    total_rows = len(df)
    total_pages = max(1, (total_rows + page_size - 1) // page_size)
    
    # Ensure page is within bounds
    page = max(1, min(page, total_pages))
    
    start_idx = (page - 1) * page_size
    end_idx = min(start_idx + page_size, total_rows)
    
    return df.iloc[start_idx:end_idx], total_pages


def reset_to_first_page():
    """Reset pagination to first page when filters change."""
    st.session_state.current_page = 1


def render_sidebar():
    """Render the sidebar with controls."""
    st.sidebar.header("Controls")
    
    # File upload
    st.sidebar.subheader("File Upload")
    uploaded_file = st.sidebar.file_uploader(
        "Choose a Parquet file",
        type=["parquet"],
        help="Upload a .parquet file to view its contents"
    )
    
    if uploaded_file is not None:
        # Load metadata only (fast)
        parquet_file, total_rows, num_row_groups, columns = load_parquet_metadata(uploaded_file)
        
        if parquet_file is not None:
            st.session_state.parquet_file = parquet_file
            st.session_state.total_rows = total_rows
            st.session_state.num_row_groups = num_row_groups
            st.session_state.columns = columns
            st.session_state.uploaded_file = uploaded_file
            
            # Show file info
            st.sidebar.success(f"Loaded: {uploaded_file.name}")
            st.sidebar.caption(f"{total_rows:,} rows, {len(columns)} columns")
    
    # Only show filter/sort/download if file is loaded
    if st.session_state.parquet_file is not None:
        columns = st.session_state.columns
        
        st.sidebar.markdown("---")
        
        # Column selection for display (column pruning)
        st.sidebar.subheader("Display Columns")
        selected_columns = st.sidebar.multiselect(
            "Select columns to display (empty = all)",
            options=columns,
            default=None,
            help="Select specific columns to improve performance for wide tables"
        )
        st.session_state.selected_columns = selected_columns if selected_columns else None
        
        st.sidebar.markdown("---")
        
        # Filtering
        st.sidebar.subheader("Filter Data")
        filter_column = st.sidebar.selectbox(
            "Select column to filter",
            options=[None] + list(columns),
            format_func=lambda x: "None" if x is None else x,
            key="filter_column_select"
        )
        st.session_state.filter_column = filter_column
        
        if filter_column:
            filter_value = st.sidebar.text_input(
                "Filter value",
                value=st.session_state.filter_value,
                help="For text columns, partial matching is supported"
            )
            st.session_state.filter_value = filter_value
        else:
            st.session_state.filter_value = ""
        
        if st.sidebar.button("Clear Filter"):
            st.session_state.filter_column = None
            st.session_state.filter_value = ""
            reset_to_first_page()
            st.rerun()
        
        st.sidebar.markdown("---")
        
        # Settings
        st.sidebar.subheader("Settings")
        with st.sidebar.expander("Performance Settings"):
            st.session_state.max_rows_full_load = st.number_input(
                "Max rows for full load",
                min_value=1000,
                max_value=1000000,
                value=st.session_state.max_rows_full_load,
                step=10000,
                help="Files with fewer rows than this will be loaded entirely into memory"
            )
            
            st.caption("Changes apply to the next file upload")


def render_schema_tab(parquet_file: pq.ParquetFile):
    """Render the schema tab content."""
    st.subheader("Schema Information")
    
    schema_df = get_schema_dataframe(parquet_file)
    st.dataframe(schema_df, use_container_width=True, hide_index=True)
    
    st.info(f"Total columns: {len(schema_df)}")


def render_metadata_tab(parquet_file: pq.ParquetFile):
    """Render the metadata tab content."""
    st.subheader("File Metadata")
    
    metadata = get_metadata_dict(parquet_file)
    
    # Display metadata in columns
    cols = st.columns(2)
    for idx, (key, value) in enumerate(metadata.items()):
        with cols[idx % 2]:
            st.metric(label=key, value=value)


def render_data_tab(parquet_file: pq.ParquetFile):
    """Render the data tab content with efficient pagination."""
    st.subheader("Data View")
    
    # Show filter status
    if st.session_state.filter_column:
        st.info(f"Filter active: {st.session_state.filter_column} contains '{st.session_state.filter_value}'")
    
    # Pagination controls
    col1, col2, col3, col4 = st.columns([2, 1, 2, 2])
    
    with col1:
        page_size = st.selectbox(
            "Rows per page",
            options=[10, 25, 50, 100, 500, 1000],
            index=1,  # Default to 25
            key="page_size_select"
        )
        st.session_state.page_size = page_size
    
    total_rows = st.session_state.total_rows
    total_pages = max(1, (total_rows + page_size - 1) // page_size)
    
    # Ensure page is within bounds
    current_page = max(1, min(st.session_state.current_page, total_pages))
    
    with col2:
        st.markdown(f"**Page {current_page} of {total_pages}**")
    
    with col3:
        if st.button("Previous Page", disabled=(current_page <= 1)):
            st.session_state.current_page = max(1, current_page - 1)
            st.rerun()
    
    with col4:
        if st.button("Next Page", disabled=(current_page >= total_pages)):
            st.session_state.current_page = min(total_pages, current_page + 1)
            st.rerun()
    
    # Read data efficiently
    with st.spinner("Loading data..."):
        if st.session_state.filter_column and st.session_state.filter_value:
            # When filtering, we need to load more data to find matches
            # For large files, this is a limitation - we scan row groups
            st.warning("Filtering loads data into memory. For very large files, this may be slow.")
            
            # Read all data and filter (for filtered view)
            table = parquet_file.read(columns=st.session_state.selected_columns)
            df = table.to_pandas()
            
            col = st.session_state.filter_column
            val = st.session_state.filter_value
            if df[col].dtype == "object":
                df = df[df[col].astype(str).str.contains(val, case=False, na=False)]
            else:
                df = df[df[col].astype(str) == val]
            
            # Paginate the filtered results
            paginated_df, total_filtered_pages = paginate_dataframe(df, current_page, page_size)
            st.session_state.current_page = min(current_page, total_filtered_pages)
            
            # Display data
            st.dataframe(paginated_df, use_container_width=True, hide_index=True)
            st.caption(f"Showing {len(paginated_df)} of {len(df)} filtered rows (total in file: {total_rows:,})")
        else:
            # No filter - use efficient row-based reading
            df = read_page_efficiently(
                parquet_file,
                current_page,
                page_size,
                columns=st.session_state.selected_columns
            )
            
            # Display data
            st.dataframe(df, use_container_width=True, hide_index=True)
            st.caption(f"Showing {len(df)} rows (total in file: {total_rows:,})")


def main():
    """Main application entry point."""
    initialize_session_state()
    
    st.title("Parquet Viewer")
    st.markdown("Upload a Parquet file to view its schema, metadata, and data.")
    
    # Render sidebar
    render_sidebar()
    
    # Main content
    if st.session_state.parquet_file is not None:
        # Create tabs - Data first as it's the most used
        tab_data, tab_schema, tab_metadata = st.tabs(["Data", "Schema", "Metadata"])
        
        with tab_data:
            render_data_tab(st.session_state.parquet_file)
        
        with tab_schema:
            render_schema_tab(st.session_state.parquet_file)
        
        with tab_metadata:
            render_metadata_tab(st.session_state.parquet_file)
    else:
        st.info("Please upload a Parquet file using the sidebar to get started.")


if __name__ == "__main__":
    main()
