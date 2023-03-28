"""Classes implementing the objects of PokeAPI."""


import requests
import os
import json
from dataclasses import dataclass, field
from typing import List, Dict, Union, Tuple


class PokeApiException(Exception):
    """Building block for ETL specific PokeAPI Exceptions."""

    pass


class WrongObjectError(PokeApiException):
    """Exception thrown when input type of object is not supported."""

    pass


WRONG_OBJECT_ERROR_MESSAGE = (
    "Wrong object type: pick from 'pokemon', 'generation' and 'evolution'."
)


@dataclass(frozen=True, slots=True, order=True)
class Pokemon:
    id: Union[int, str]
    name: str
    stats: Dict[str, int]
    abilities: List[Dict[str, bool]]
    types: List[str]
    sort_index: float = field(init=False, repr=False)

    def __post_init__(self):
        """Will assign 'coverage' as sort index.
        Example:
        p1 = Pokemon(1, 'charmander', [('hp', 45), ('attack', 13)], [{'rake': True}], ['normal'])
        p2 = Pokemon(3, 'charizard', [('hp', 89), ('attack', 27)], [{'rake': True, 'burn': False}], ['normal', 'fire'])
        p1 < p2 -> False
        """
        object.__setattr__(self, "sort_index", len(self.stats) / len(self.abilities))

    @staticmethod
    def parse_pokemon_stats(pokemon: Dict) -> Union[None, Dict[str, Tuple[int, int]]]:
        """Parse a pokemon's stats JSON object and chisel it to a more practical format.
        Args:
            pokemon (Dict): a JSON object describing various Pokemon characteristics.
        Return:
            (Dict[str, Tuple[int, int]]): If valid input, dict with stat names as keys
            and tuples of (base_stat, effort) as values.
            (None): Returns None if input Pokemon object not valid.
        """
        if pokemon:
            stats_obj = pokemon["stats"]
            pokemon_stats = [(s["stat"]["name"], s["base_stat"]) for s in stats_obj]

            return pokemon_stats
        else:
            return None

    @staticmethod
    def parse_pokemon_abilities(
        pokemon: Dict,
    ) -> Union[None, Dict[str, Tuple[str, int, bool]]]:
        """Parse a pokemon's  JSON object and single out the abilities.
        Args:
            pokemon (Dict): a JSON object describing various Pokemon characteristics.
        Return:
            (Dict[str, Tuple[str, int, bool]]): If valid input, dict with ability names as keys
            and tuples of (ability_name, ability_id, is_hidden) as values.
            (None): Returns None if input Pokemon object not valid.
        """
        if pokemon:
            abilities_obj = pokemon["abilities"]
            pokemon_abilities = [
                (a["ability"]["name"], a["is_hidden"]) for a in abilities_obj
            ]
            return pokemon_abilities
        else:
            return None

    @staticmethod
    def parse_pokemon_types(pokemon: Dict) -> Union[None, List[str]]:
        """Parse a pokemon's JSON object and single out the types.
        Args:
            pokemon (Dict): a JSON object describing various Pokemon characteristics.
        Return:
            (Dict[str, Tuple[int, int]]): If valid input, list of type names.
            (None): Returns None if input Pokemon object not valid.
        """
        if pokemon:
            types_obs = pokemon["types"]

            return [t["type"]["name"] for t in types_obs]
        else:
            return None


@dataclass(frozen=True, slots=True, order=True)
class Generation:
    id: Union[int, str]
    name: str
    species: List[str]
    abilities: List[str]
    sort_index: int = field(init=False, repr=False)

    def __post_init__(self):
        """Will assign ID as sort index.
        Example:
        g1 = Generation(1, 'g1', ['a', 'b'], ['a', 'b'])
        g2 = Generation(2, 'g1', ['a', 'b'], ['a', 'b'])
        g1 > g2 -> False
        """
        object.__setattr__(self, "sort_index", self.id)

    @staticmethod
    def parse_generation_species(
        generation: Dict,
    ) -> Union[None, List[str]]:
        """Parse a generation's JSON object and single out the Pokemon species.
        Args:
            pokemon (Dict): a JSON object describing various Pokemon generation characteristics.
        Return:
            (List[str]): If valid input, dict with ability names as keys
            and tuples of (species_name, species_id) as values.
            (None): Returns None if input Pokemon object not valid.
        """
        if generation:
            generation_species_obj = generation["pokemon_species"]

            return [a["name"] for a in generation_species_obj]
        else:
            return None

    @staticmethod
    def parse_generation_abilities(
        generation: Dict,
    ) -> Union[None, List[str]]:
        """Parse a generation's JSON object and single out the abilities.
        Args:
            pokemon (Dict): a JSON object describing various Pokemon generation characteristics.
        Return:
            (List[str]): If valid input, dict with ability names as keys
            and tuples of (ability_name, ability_id) as values.
            (None): Returns None if input Pokemon object not valid.
        """
        if generation:
            generation_abilities_obj = generation["abilities"]

            return [a["name"] for a in generation_abilities_obj]
        else:
            return None


@dataclass(frozen=True, slots=True)
class Evolution:
    id: Union[int, str]
    species: List[str]

    @staticmethod
    def parse_evolution_chain(evolution: Dict) -> List[str]:
        """Parse an Evolution object and get all species names in the chain.
        Args:
            evolution (Dict): an evolution JSON object as given by API.
        Return:
            list (str): a list of Pokemon names.
        """
        evolution_chain = []
        evolution_data = evolution["chain"]

        while evolution_data and "evolves_to" in evolution_data:
            evolution_chain.append(evolution_data["species"]["name"])

            try:
                evolution_data = evolution_data["evolves_to"][0]
            except IndexError:
                evolution_data = None

        return evolution_chain


class RequestHandler:
    """Static class with no attributes. Implements methods for processing request to Pokeapi."""

    @staticmethod
    def process_api_get_response(url: str) -> Union[None, Dict[str, Union[Dict, List]]]:
        """Process a GET response from the API.
        Args:
            url (str): an URL entrypoint to an API GET method.
        Return:
            (None): if the response status code was not 200 (valid).
            (Union[None, Dict[str, Union[Dict, List]]]): if the response status code was 200.
        """
        try:
            response = requests.get(url)
        except requests.exceptions.SSLError:
            return None

        if response.status_code != 200:
            data = response.text
        else:
            data = response.json()

        if isinstance(data, str):
            return None
        else:
            return data

    @staticmethod
    def get_object_from_id(
        object_id: Union[int, str], object_type: str
    ) -> Union[None, Dict[str, Union[Dict, List]]]:
        """Starting from (numeric) ID (up to 9 presently), retrieve its JSON object.
        Args:
            generation_id (int): integer describing the ID of the Pokemon generation as known by the server database.
        Returns:
            (None): if the response code was not 202, the return object is empty.
            (Union[None, Dict[str, Union[Dict, List]]]): a JSON object with data if response code was 202.
        """
        base_url = "https://pokeapi.co/api/v2/"

        if object_type not in ("pokemon", "generation", "evolution"):
            raise WrongObjectError(WRONG_OBJECT_ERROR_MESSAGE)
        else:
            if object_type == "evolution":
                object_type = "evolution-chain"
            return RequestHandler.process_api_get_response(
                f"{base_url}/{object_type}/{object_id}/"
            )

    @staticmethod
    def get_all_objects_ids(
        object_type: str, requests_limit: int = 10_000_000
    ) -> List[str]:
        """Get all resource IDs in existence as a list of strings.
        Args:
            object_type (str): defines the object to scrape; Accepted values are 'pokemon',
            'generation' and 'evolution'.
            requests_limit(int): limit of pages to parse. Defaults to 10_000_000.
        Return:
            (List[str]): list of IDs.
        """
        if object_type not in ("pokemon", "generation", "evolution"):
            raise ValueError(
                "Wrong object type: pick from 'pokemon', 'generation' and 'evolution'."
            )
        if object_type == "evolution":
            object_type = "evolution-chain"

        get_url = f"http://pokeapi.co/api/v2/{object_type}/?limit={requests_limit}"

        return [
            r["url"].split("/")[-2]
            for r in RequestHandler.process_api_get_response(get_url)["results"]
        ]

    @staticmethod
    def parse_pokeapi_object(
        object_id: Dict, object_type: str, file_path: str = None
    ) -> Dict[str, Union[str, Union[int, float, List[Union[int, float, str]]]]]:
        """Parse a Pokemon API JSON response object to extract specific info.
        Args:
            object_id (int): an ID.
            object_type (str): string indicating how to parse the JSON object. Support for:
            'pokemon', 'generation' and 'evolution'
            file_path (str): path where to save. Defaults to None, in which case saving is not
            attempted.
        Return:
            (Dict[str, Union[str, Union[int, float, List[Union[int, float, str]]]]]:): a JSON object with keys
            as the different data points, which can then be either be numerical or lists of numbers and strings.
        """
        data = RequestHandler.get_object_from_id(object_id, object_type)

        if data:
            if object_type == "pokemon":
                id = data["id"]
                name = data["name"]
                stats = Pokemon.parse_pokemon_stats(data)
                abilities = Pokemon.parse_pokemon_abilities(data)
                types = Pokemon.parse_pokemon_types(data)
                result = Pokemon(id, name, stats, abilities, types)
            elif object_type == "generation":
                id = data["id"]
                name = data["name"]
                species = Generation.parse_generation_species(data)
                abilities = Generation.parse_generation_abilities(data)
                result = Generation(id, name, species, abilities)
            elif object_type == "evolution":
                id = data["id"]
                species = Evolution.parse_evolution_chain(data)
                result = Evolution(id, species)
            else:
                raise WrongObjectError(WRONG_OBJECT_ERROR_MESSAGE)

            if file_path:
                # TODO: this saves each object as a list of JSON. Not Ideal.
                # try to save as just one JSON
                file_path = f"{file_path}/{object_type}/{result.id}.json"

                if not os.path.exists(file_path):
                    basedir = os.path.dirname(file_path)
                    if not os.path.exists(basedir):
                        os.makedirs(basedir)
                with open(file_path, "a") as f:
                    os.utime(file_path, None)
                with open(file_path, "r") as f:
                    try:
                        objs = json.load(f)
                    except json.decoder.JSONDecodeError:
                        objs = []
                # Using sots to access the attributes is over 20% than __dict__.
                # however, we need to add a workaround to access the data in JSON format.
                # Furthermore, I don't want to include the sort index in the final dataframes.
                objs.append(
                    {
                        slot: getattr(result, slot)
                        for slot in result.__slots__
                        if slot != "sort_index"
                    }
                )
                with open(file_path, "w") as f:
                    json.dump(objs, f, indent=4)
                return None
            else:
                return result
        else:
            return None
