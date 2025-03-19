# Data Types

This guide will focus on the structure of the JSON data types at the core of the repo.

The base structure for each JSON type is the [`DataModelObject`](https://github.com/clinical-biomarkers/format-converter/blob/b0a0ad8e88d61a08ff22bd491e4f7524a704617e/utils/data_types/json_types.py#L14) abstract class. The `DataModelObject` specifies two abstract methods: 

1. `to_dict`: defines how the object can be converted to a JSON serializable Python dictionary 
2. `from_dict`: defines how to build the object given a JSON serializable Python dictionary

There are two additional abstract classes:


The [`CacheableDataModelObject`](https://github.com/clinical-biomarkers/format-converter/blob/b0a0ad8e88d61a08ff22bd491e4f7524a704617e/utils/data_types/json_types.py#L27) abstract class defines an object that can have its metadata cached (more information on metadata retrieval can be found [here](./Metadata.md). The `CacheableDataModelObject` specifies three abstract methods:

1. `to_cache_dict`: defines a standardized way to save an entry to the resource's cache file
2. `from_cache_dict`: defines how to build the object given an entry from the cache file
3. `type_guard`: defines a checker method to ensure that an object is in fact of that type

The [`RecommendedNameObject`](https://github.com/clinical-biomarkers/format-converter/blob/b0a0ad8e88d61a08ff22bd491e4f7524a704617e/utils/data_types/json_types.py#L47) abstract class defines an object which can compare a name with the resource recommended name.
