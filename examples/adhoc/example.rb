#!/usr/bin/env ruby

require 'json'

metrics = {}


dimensions = {"dim1" => "val1"}

metrics['first'] = {
    "name" => "example",
    "value" => 2.0,
    "dimensions" => dimensions,
    "metricType"=> "gauge"
}

metrics['second'] = {
    "name" => "anotherExample",
    "value" => 2.0,
    "dimensions" => dimensions,
    "metricType" => "cumcounter"
}

puts metrics['first'].to_json

puts metrics.values.to_json
