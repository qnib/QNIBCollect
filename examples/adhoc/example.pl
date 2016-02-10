#!/usr/bin/env perl
use strict;
use warnings;

use JSON qw( encode_json );

my %metrics = ();
my $dimensions = {"dim1" => "val1"};

$metrics{"first"} = {
    "name" => "example",
    "value" => 2.0,
    "dimensions" => $dimensions,
    "metricType" => "gauge"
};

$metrics{"second"} = {
    "name" => "anotherExample",
    "value" => 2.0,
    "dimensions" => $dimensions,
    "metricType" => "cumcounter"
};

my @metric_vals = values %metrics;

# Send one metric
print STDOUT encode_json \%{$metrics{"first"}};

# Send them all
print STDOUT encode_json \@metric_vals;
