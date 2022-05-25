#!/usr/bin/env python3

"""
This script generate 10 data sources, 200 anchor features,
and 400 derived features which depend on anchor features
and other derived features.
Each anchor feature depends on 1 data source.
Each derived feature has 1~4 dependencies, and the dependency
depth is 1~3.
The entity key is UUID, mimics Azure PurView behaviors.
The entity payload is minimized to contain `name` and `type` only,
without any other fields.
"""

import random
import uuid

# Generate 10 sources
n = 10
source_ids = [str(uuid.uuid4()) for _ in range(n)]
for i in range(n):
    print("insert into entities (entity_id, entity_name, entity_type) values ('%s', 'source%d', 'Source')" % (source_ids[i], i))

# Generate 200 anchor features
n = 200
anchor_feature_ids = [str(uuid.uuid4()) for _ in range(n)]
for i in range(n):
    print("insert into entities (entity_id, entity_name, entity_type) values ('%s', 'anchor_feature%d', 'AnchorFeature')" % (anchor_feature_ids[i], i))
    # Connect anchor feature and source
    to_id = source_ids[random.randrange(10)]
    print("insert into entity_dep (from_id, to_id, conn_type) values ('%s', '%s', 'Consumes')" % (anchor_feature_ids[i], to_id))
    print("insert into entity_dep (from_id, to_id, conn_type) values ('%s', '%s', 'Produces')" % (to_id, anchor_feature_ids[i]))

print('-' * 100)

# Generate 400 derived features, each has 1~4 dependencies

# Generate 1st 100 derived features depend on anchor features directly
n = 100
derived_feature_ids_1 = [str(uuid.uuid4()) for _ in range(n)]
for i in range(n):
    print("insert into entities (entity_id, entity_name, entity_type) values ('%s', 'derived_feature%d', 'DerivedFeature')" % (derived_feature_ids_1[i], i))
    # 1~4 upstream features
    for _ in range(random.randrange(1, 5)):
        # Connect derived feature and anchor feature
        to_id = anchor_feature_ids[random.randrange(len(anchor_feature_ids))]
        print("insert into entity_dep (from_id, to_id, conn_type) values ('%s', '%s', 'Consumes')" % (derived_feature_ids_1[i], to_id))
        print("insert into entity_dep (from_id, to_id, conn_type) values ('%s', '%s', 'Produces')" % (to_id, derived_feature_ids_1[i]))

print('-' * 100)

# Generate 2nd 100 derived features depend on anchor features and derived_feature_ids_1
n = 100
dep1 = anchor_feature_ids + derived_feature_ids_1
derived_feature_ids_2 = [str(uuid.uuid4())for _ in range(n)]
for i in range(n):
    print("insert into entities (entity_id, entity_name, entity_type) values ('%s', 'derived_feature%d', 'DerivedFeature')" % (derived_feature_ids_2[i], i+len(dep1)))
    # 1~4 upstream features
    for _ in range(random.randrange(1, 5)):
        # Connect derived feature and anchor feature
        to_id = dep1[random.randrange(len(dep1))]
        print("insert into entity_dep (from_id, to_id, conn_type) values ('%s', '%s', 'Consumes')" % (derived_feature_ids_2[i], to_id))
        print("insert into entity_dep (from_id, to_id, conn_type) values ('%s', '%s', 'Produces')" % (to_id, derived_feature_ids_2[i]))

print('-' * 100)

# Generate 3rd 100 derived features depend on anchor features and derived_feature_ids_1 and 2
n = 100
dep2 = anchor_feature_ids + derived_feature_ids_1 + derived_feature_ids_2
derived_feature_ids_3 = [str(uuid.uuid4()) for _ in range(n)]
for i in range(n):
    print("insert into entities (entity_id, entity_name, entity_type) values ('%s', 'derived_feature%d', 'DerivedFeature')" % (derived_feature_ids_3[i], i+len(dep2)))
    # 1~4 upstream features
    for _ in range(random.randrange(1, 5)):
        # Connect derived feature and anchor feature
        to_id = dep2[random.randrange(len(dep2))]
        print("insert into entity_dep (from_id, to_id, conn_type) values ('%s', '%s', 'Consumes')" % (derived_feature_ids_3[i], to_id))
        print("insert into entity_dep (from_id, to_id, conn_type) values ('%s', '%s', 'Produces')" % (to_id, derived_feature_ids_3[i]))

# Generate 4th 100 derived features depend on anchor features and derived_feature_ids_1, 2, 3
n = 100
dep3 = anchor_feature_ids + derived_feature_ids_1 + derived_feature_ids_2 + derived_feature_ids_3
derived_feature_ids_4 = [str(uuid.uuid4()) for _ in range(n)]
for i in range(n):
    print("insert into entities (entity_id, entity_name, entity_type) values ('%s', 'derived_feature%d', 'DerivedFeature')" % (derived_feature_ids_4[i], i+len(dep3)))
    # 1~4 upstream features
    for _ in range(random.randrange(1, 5)):
        # Connect derived feature and anchor feature
        to_id = dep3[random.randrange(len(dep3))]
        print("insert into entity_dep (from_id, to_id, conn_type) values ('%s', '%s', 'Consumes')" % (derived_feature_ids_4[i], to_id))
        print("insert into entity_dep (from_id, to_id, conn_type) values ('%s', '%s', 'Produces')" % (to_id, derived_feature_ids_4[i]))
