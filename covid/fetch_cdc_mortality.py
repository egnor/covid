"""Module to retrieve mortality data from the CDC WONDER database."""

import io

import pandas


# TODO - figure out how to load from CDC rather than hardcoding.
CANNED_DATA = """
"Notes"	"State"	"State Code"	Deaths	Population	Crude Rate
	"Alabama"	"01"	54352	4887871	1112.0
	"Alaska"	"02"	4453	737438	603.8
	"Arizona"	"04"	59282	7171646	826.6
	"Arkansas"	"05"	32336	3013825	1072.9
	"California"	"06"	268818	39557045	679.6
	"Colorado"	"08"	38526	5695564	676.4
	"Connecticut"	"09"	31230	3572665	874.1
	"Delaware"	"10"	9433	967171	975.3
	"District of Columbia"	"11"	5008	702455	712.9
	"Florida"	"12"	205426	21299325	964.5
	"Georgia"	"13"	85202	10519475	809.9
	"Hawaii"	"15"	11415	1420491	803.6
	"Idaho"	"16"	14261	1754208	813.0
	"Illinois"	"17"	110022	12741080	863.5
	"Indiana"	"18"	65693	6691878	981.7
	"Iowa"	"19"	30367	3156145	962.2
	"Kansas"	"20"	27537	2911505	945.8
	"Kentucky"	"21"	48707	4468402	1090.0
	"Louisiana"	"22"	46048	4659978	988.2
	"Maine"	"23"	14715	1338404	1099.4
	"Maryland"	"24"	50568	6042718	836.8
	"Massachusetts"	"25"	59152	6902149	857.0
	"Michigan"	"26"	98903	9995915	989.4
	"Minnesota"	"27"	44745	5611179	797.4
	"Mississippi"	"28"	32301	2986530	1081.6
	"Missouri"	"29"	63117	6126452	1030.2
	"Montana"	"30"	9992	1062305	940.6
	"Nebraska"	"31"	16904	1929268	876.2
	"Nevada"	"32"	24715	3034392	814.5
	"New Hampshire"	"33"	12774	1356458	941.7
	"New Jersey"	"34"	75765	8908520	850.5
	"New Mexico"	"35"	19007	2095428	907.1
	"New York"	"36"	157183	19542209	804.3
	"North Carolina"	"37"	93885	10383620	904.2
	"North Dakota"	"38"	6445	760077	847.9
	"Ohio"	"39"	124264	11689442	1063.0
	"Oklahoma"	"40"	40933	3943079	1038.1
	"Oregon"	"41"	36187	4190713	863.5
	"Pennsylvania"	"42"	134702	12807060	1051.8
	"Rhode Island"	"44"	10083	1057315	953.6
	"South Carolina"	"45"	50640	5084127	996.0
	"South Dakota"	"46"	7971	882235	903.5
	"Tennessee"	"47"	71078	6770010	1049.9
	"Texas"	"48"	202211	28701845	704.5
	"Utah"	"49"	18354	3161105	580.6
	"Vermont"	"50"	6027	626299	962.3
	"Virginia"	"51"	69359	8517685	814.3
	"Washington"	"53"	56877	7535591	754.8
	"West Virginia"	"54"	23478	1805832	1300.1
	"Wisconsin"	"55"	53684	5813568	923.4
	"Wyoming"	"56"	5070	577737	877.6
"""


def get_states(session):
    """Returns a pandas.DataFrame of state-level mortality data."""

    data = pandas.read_csv(io.StringIO(CANNED_DATA), sep="\t")

    data = data[data["State Code"].notna()]
    data.drop("Notes", axis="columns", inplace=True)
    data.set_index("State Code", inplace=True)
    return data


def credits():
    return {"https://wonder.cdc.gov/": "CDC WONDER"}


if __name__ == "__main__":
    import argparse
    from covid import cache_policy

    parser = argparse.ArgumentParser(parents=[cache_policy.argument_parser])
    states = get_states(session=cache_policy.new_session(parser.parse_args()))
    print(states)
