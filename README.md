# Merlin

---
title: Metrics
---

# Metrics

Here we want to discuss how to centralize metrics.

Ideally we want to have all the metrics in one place so that we can use them for monitoring but also as input for other systems.

In the past the common way to do this is to create dashboards that merge various metrics calculated in different systems. This unified view is just at the presentation layer but the data and the logic is still scattered around.

The idea is to centralize storage and definition. Here storage is not about the medium, different metrics can be stored in different media. Storage, in this case ,is about schema. The idea is to have a common schema for all the metrics.

The other core point is the definition of a metric. Metrics are artifacts that we define and therefore are subject to be redefined over time. This is quite problematic as we might forget how the metric was defined. Even when we capture the definition in some other document or system there is the problem to link the two system in such way that can be accessed through code.

Therefore we want to have 2 key requirement for metrics:

1. Common schema
2. Include the definition through the measurement of the metric.

## Hierarchy

We can start with a practical example. Let's say we want measure the total weight of oil barrels extracted from various oil wells. We can place each barrel on a barrel weighing machine and record the weight (our metric). Even in the most simplistic of the scenarios, where all the barrels are indistinguishable (same size) and we have one measuring machine, there are other factors that might impact the weight (our metric). For example, the type of the oil, temperature, humidity, when the scale was calibrated can play a role in the value of the metric. Therefore when we measure we might want to capture those factors as well as the metric itself. Some of those factors might be totally independent, some of them might have some hierarchy. In our example we would end up collecting the data in a tabular format that could look like this

```
Time                  Location    Temperature    Humidity    Time since calibration    Weight
2020-01-01T03:00:00       1          35           0.9          3000                    138.33
2020-01-01T03:05:00       1          35.5         0.92         3300                    138.2
2020-01-01T03:10:00       1          35.5         0.92         3900                    138.6
2020-01-01T03:15:00       1          35           0.9          4200                    138.55
2020-01-01T03:00:00       2          20           0.3          200                     139.7
2020-01-01T03:05:00       2          21           0.3          500                     139.9
2020-01-01T03:10:00       2          21           0.31         800                     139.7
2020-01-01T03:15:00       2          20           0.03         1100                    139.8
```

For different scenarios we might be interested to aggregate the data a different levels. For example, we might want to know per hour across all location the total weight. We call this metric 0 (M1). For another scenarios we might need to know total weight for each locations while binning the temperature at 1C interval we call this (M2)

It easy to say that that M1 and M2 will be different despite the share both the source data and the definition (total weight). The difference is what attribute we use to aggregate the data on. Rather than defining two metrics we want to capture this relationship between M1 and M2\. Therefore our schema we want to be able to capture this hierarchy.

## Functional variables and expression

Let's think again about our M2 metric. The metric is still the `sum(Weight)` aggregate by location and temperature binned at 1C. When we aggregate by location we do not transform it, however we need to round the temperature to +/- 1C. As we might use different way of rounding it would be best to record this information with the metric itself. Since this information is not an aggregate value but rather an expression and a variable we would rather clearly identify them as `functional variable` and `function expression`

## Core attributes of a metric

We can now outline the core attributes of a metric:

1. `id` a string that identifies the metric
2. `time` when the metric was recorded
3. `w_time` how big is the time window between 2 consecutive measurements
4. `group_map` a map that contains each variable used to aggregate the metrics
5. `group_keys` list of facets used for grouping
6. `func_map` a map each functional variables
7. `func_expr` a string describing the definition of the metric
8. `compute_time` when the metric was computed
9. `v_lvl` vertical hierarchical level
10. `h_lvl` horizontal hierarchical levels

## Capturing different types in group_map

One technical constrain is that in many system map needs to have specific types. Specifically in SQL map needs to have a defined type e.g. `map<string, string>` or `map<string, long>`

Therefore `group_map` and `func_map` need to be defined in such a way that it will support common data types.

...
