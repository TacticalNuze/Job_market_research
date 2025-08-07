print("--- superset_config.py loaded successfully ---")

CACHE_CONFIG = {"CACHE_TYPE": "null"}

FEATURE_FLAGS = {
    "ENABLE_EXPLORE_DRAG_AND_DROP": True,
    "DASHBOARD_NATIVE_FILTERS": True,
    "DASHBOARD_CROSS_FILTERS": True,
    "DASHBOARD_NATIVE_FILTERS_SET": True,
    "GENERIC_CHART_AXES": True,
    "ENABLE_ECHARTS": True,
    "EMBEDDED_SUPERSET": True,
}

SECRET_KEY = "testkey"
EXTRA_CATEGORICAL_COLOR_SCHEMES = [
    {
        "id": "dxccolors",
        "description": "Colors used by DxC",
        "label": "DxC Color Scheme",
        "isDefault": True,
        "colors": [
            "#5F249F",
            "#D9D9D9",
            "#FFFFFF",
            "#969696",
            "#63666A",
            "#000000",
            "#00968F",
            "#00A3E1",
            "#006975",
            "#6CC24A",
            "#ED9B33",
            "#FFCD00",
            "#330072",
            "#F9F048",
        ],
    }
]
