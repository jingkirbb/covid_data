use anyhow::{Context, Result};
use chrono::{TimeZone, Utc};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::path::PathBuf;
use structopt::StructOpt;

type Rfc3339 = String;

#[derive(Debug, StructOpt)]
#[structopt(name = "example", about = "An example of StructOpt usage.")]
struct Opt {
    /// Input file containing covid API response
    // https://www.knowi.com/coronavirus-dashboards/covid-19-api/
    //
    // per county level:
    // curl https://knowi.com/api/data/ipE4xJhLBkn8H8jisFisAdHKvepFR5I4bGzRySZ2aaXlJgie\?entityName\=Raw%20County%20level%20Data\&exportFormat\=json
    #[structopt(parse(from_os_str))]
    input: PathBuf,

    #[structopt(parse(from_os_str))]
    output_dir: PathBuf,
}

#[derive(Deserialize)]
struct CovidCountyRawDataEntry {
    #[serde(rename(deserialize = "Date"))]
    pub date: i64,
    #[serde(rename(deserialize = "County"))]
    pub county: String,
    #[serde(rename(deserialize = "State"))]
    pub state: String,

    pub values: i64,

    #[serde(rename(deserialize = "Type"))]
    pub entry_type: String,
}

#[derive(Debug)]
struct CountyEntry {
    name: String,
    state: String,
    confirmed: i64,
    deaths: i64,
}

#[derive(Serialize, Debug, Default)]
struct Node {
    name: String,
    metrics: BTreeMap<&'static str, i64>,
    edges_directed: BTreeSet<String>,
    extra_fields: BTreeMap<&'static str, String>,
}

impl Node {
    pub fn add_metric(&mut self, m: &'static str, v: i64) {
        if !self.metrics.contains_key(m) {
            self.metrics.insert(m, 0);
        }
        *self.metrics.get_mut(m).unwrap() += v;
    }
}

#[derive(Serialize, Debug, Default)]
struct Graph {
    timestamp: Rfc3339,
    nodes: Vec<Node>,
}

fn main() -> Result<()> {
    let l = ll::Logger::stdout();
    let Opt { input, output_dir } = Opt::from_args();

    let raw_data = l.event("read_file", |e| {
        let data = fs::read(input).context("Failed to read raw covid JSON data")?;
        e.add_data("size MB", data.capacity() / 1000000);
        Ok(data)
    })?;

    let data = l.event("parse", |e| {
        let result = serde_json::from_slice::<Vec<CovidCountyRawDataEntry>>(&raw_data[..])
            .context("Failed to parse JSON")?;
        e.add_data("entries", result.len());
        Ok(result)
    })?;

    let grouped = l.event("group by", |_| {
        let result =
            data.into_par_iter()
                .fold(
                    || HashMap::new(),
                    |mut result, entry| {
                        // raw data is in milisseconds
                        let date = Utc.timestamp(entry.date / 1000, 0);
                        let date_entry = result.entry(date).or_insert_with(HashMap::new);
                        let state = entry.state;
                        let name = entry.county;
                        // Make sure we namespace by state in case there are similar county names
                        let county_key = format!("{} - {}", &state, &name);
                        let county_entry =
                            date_entry.entry(county_key).or_insert_with(|| CountyEntry {
                                name,
                                state,
                                confirmed: 0,
                                deaths: 0,
                            });

                        match entry.entry_type.as_str() {
                            "Confirmed" => county_entry.confirmed += entry.values,
                            "Deaths" => county_entry.deaths += entry.values,
                            _ => (),
                        }
                        result
                    },
                )
                .reduce(
                    || HashMap::new(),
                    |from, mut into| {
                        for (date, from_county_entries) in from {
                            let into_date_entry = into.entry(date).or_insert_with(HashMap::new);

                            for (county, from_county_entry) in from_county_entries {
                                let from_confirmed = from_county_entry.confirmed;
                                let from_deaths = from_county_entry.deaths;

                                let into_county_entry = into_date_entry
                                    .entry(county)
                                    .or_insert_with(|| CountyEntry {
                                        name: from_county_entry.name,
                                        state: from_county_entry.state,
                                        confirmed: 0,
                                        deaths: 0,
                                    });

                                into_county_entry.confirmed += from_confirmed;
                                into_county_entry.deaths += from_deaths;
                            }
                        }
                        into
                    },
                );
        Ok(result)
    })?;

    let with_state_nodes = l.event("add state nodes", |_| {
        let nodes_by_date = grouped
            .into_par_iter()
            .map(|(date, entries)| {
                let mut states = HashMap::new();

                let mut all_nodes: Vec<Node> = Vec::new();

                for (key, county_entry) in entries {
                    if !states.contains_key(&county_entry.state) {
                        states.insert(
                            county_entry.state.clone(),
                            Node {
                                name: county_entry.state.clone(),
                                ..Default::default()
                            },
                        );
                    }
                    let state_entry = states
                        .get_mut(&county_entry.state)
                        .expect("state must be there");

                    state_entry.add_metric("confirmed", county_entry.confirmed);
                    state_entry.add_metric("deaths", county_entry.deaths);
                    state_entry.edges_directed.insert(key.clone());

                    all_nodes.push(Node {
                        name: key,
                        metrics: vec![
                            ("confirmed", county_entry.confirmed),
                            ("deaths", county_entry.deaths),
                        ]
                        .into_iter()
                        .collect(),
                        extra_fields: vec![("display_name", county_entry.name)]
                            .into_iter()
                            .collect(),
                        edges_directed: BTreeSet::new(),
                    })
                }

                for (_, state) in states {
                    all_nodes.push(state);
                }

                Graph {
                    timestamp: date.to_rfc3339(),
                    nodes: all_nodes,
                }
            })
            .collect::<Vec<Graph>>();

        Ok(nodes_by_date)
    })?;

    l.event("write_files", |e| {
        e.add_data("output_dir", output_dir.display().to_string());
        e.add_data("num_files", with_state_nodes.len());

        with_state_nodes
            .into_par_iter()
            .map(|graph| {
                let mut filepath = output_dir.clone();
                filepath.push(&graph.timestamp);

                let json = serde_json::to_string_pretty(&graph)?;

                fs::write(filepath, json)?;
                Ok(())
            })
            .collect::<Result<()>>()?;
        Ok(())
    })?;
    Ok(())
}
