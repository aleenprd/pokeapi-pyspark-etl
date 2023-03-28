# PokeAPI ETL
This is a PySpark ETL extracting data via PokeAPI, transforming it for the purpose of specific analytics and loading raw and transformed results to local storage.
<br><br>

## 1. The Analytical Questions<hr>
Let's imagine management has taken up a curious interest in Pokemon and they have some questions for their data department to solve.

1. What’s the <b>average for each base stats for every Pokémon type across all generations</b>?
    - Need to know all Pokemon names.
    - Need to know all Pokemon types.
    - Need to know all generations.
    - Need to know all base stat types.

2. Find the <b>Pokémon with the best type coverage per generation</b>.
    - Need to algorithmically define the coverage = can learn the most amount of moves with different types.
    - Need to know the generations.

3. For all abilities find all Pokémon that have the ability as <b>a hidden ability</b> and all Pokémon that have it as <b>a non-hidden ability</b>.
    - Need to know all abilities.
    - Need to break them down into hidden and not hidden abilities.

4. Find Pokémon with similar move-set that do not belong to the same evolution chain and are found in different generations.
    - Need to define move set = set of abilities.
    - Need to know evolution chains and generations.


## 2. Poke-API
<hr>
Using this API, you can consume information on Pokémon, their moves, abilities, types, egg groups and much, much more. A RESTful API is an API that conforms to a set of loose conventions based on HTTP verbs, errors, and hyperlinks.

How much information is stored within the API? It currently has tens of thousands of individual items in their database, including:
- Moves
- Abilities
- Pokémon (including various forms)
- Types
- Egg Groups
- Game Versions
- Items
- Pokédexes
- Pokémon Evolution Chains

<b>Notes on functionality of API:</b>
- This is a consumption-only API — only the HTTP GET method is available on resources.
- No authentication is required to access this API, and all resources are fully open and available. 
- Rate limiting has been removed entirely
<br><br>

## 3. The API's Data
<hr>
The data this pipeline will be working with is the following:
<br><br>

### Pokemon
Pokémon are the creatures that inhabit the world of the Pokémon games. They can be caught using Pokéballs and trained by battling with other Pokémon. Each Pokémon belongs to a specific species but may take on a variant which makes it differ from other Pokémon of the same species, such as base stats, available abilities and typings. 
- GET: https://pokeapi.co/api/v2/pokemon/{id_or_name}/

### Pokemon Abilities
Abilities provide passive effects for Pokémon in battle. Pokémon have multiple possible abilities but can have only one ability at a time.
- GET: https://pokeapi.co/api/v2/ability/{id_or_name}/

### Pokemon Generations
A generation is a grouping of the Pokémon games that separates them based on the Pokémon they include. In each generation, a new set of Pokémon, Moves, Abilities and Types that did not exist in the previous generation are released.
- GET: https://pokeapi.co/api/v2/generation/{id_or_name}/

### Pokemon Stats
Stats determine certain aspects of battles. Each Pokémon has a value for each stat which grows as they gain levels and can be altered momentarily by effects in battles.
- GET: https://pokeapi.co/api/v2/stat/{id_or_name}/

### Pokemon Ability Types
Types are properties for Pokémon and their moves. Each type has three properties: which types of Pokémon it is super effective against, which types of Pokémon it is not very effective against, and which types of Pokémon it is completely ineffective against.
- GET: https://pokeapi.co/api/v2/type/{id_or_name}/
<br><br>

## 4. Building the Project & How to Use
<hr>
You can build the project by using Pipenv:

- To create a virtual environment write `pipenev shell`.
- To install the required packages write `pipenev install`. This will install packages from Pipfile.lock.
- `deactivate` in order to end the virtual environment. If it persists, `exit` will do the  trick.

The project was built with the following design choices in mind:
- The classes and methods used are packaged inside the <i>'src/dependencies'</i> folder.
- There is an <i>'etl'</i> module which establishes an abstract class on which mature ETLs should be inherit and be built on.
- The different objects and methods that revolve around the API are grouped in the <i>'objects'</i> module. You will notice use of Python's new data classes.
- The mature ETL is packaged in the <i>'pokeapi'</i> module. It defines the Extract, Transform and Load methods.
- Finally, the <i>'src/main.py'</i> file is the entrypoint for the application. You can `cd src` and execute `python main.py` with default arguments or change the application's command line arguments (<i>extract_save_path, cosine_threshold, load_save_path, spark_driver_memory and spark_executor_memory</i>).
- Furthermore, there is an additional Jupyter Notebook called <i>'analysis.ipynb'</i> where I present the transformed datasets which were used to answer the task' questions. 

Executing main.py will trigger a series of actions:
- First, it will validate input configurations and set up a Spark Session.
- Then, with the configurations for the ETL, it will commence the Extract phase.
- In Extract, we first fetch all IDs for Pokemon, Generations and Evolutions and then we use Multithreading on the driver to fetch the object for each ID concurrently, simultaneously pre-processing the JSONs with Python and saving each to its own JSON file. This is a deliberate design choice, in order to mitigate having too much data collect into the driver's RAM. 
- After saving the  'raw' results, Spark will read the JSON files into Spark SQL Dataframes.
- Then Transform stage commences and manipulates the three dataframes, creating 4 more dataframes, one for each task, including the optional ones.
- Finally, the Load stage will go through the dataframes and save each one to a specified location, in Parquet format.

## 5. Potential Improvements
<hr>

- Find a way to distribute the Extract method over the executors instead of using the Driver's threads. In practice, since we're handling low volume per requests, the only bottleneck is speed if, say we had millions of requests to be made, we would like to scale horizontally.
- Write unit and integration tests for the ETL, as well as schema validations for each stage. Ideally, you would throw a lot of unexpected inputs at the tests, to force errors and account for unwanted situations.
- Declare custom __post_init__ methods and put all checks there to force type checking. Data classes use type hinting but they can still be instantiated with erroneous values. This would enforce quality data flow throughout the pipeline. 
- Find ways to avoid Cartesian Products on Cosine Similarity between Pokemon abilities - very expensive operation.
- We can easily Dockerize the application and deploy it in a container, in a pod.
- Add Logging instead of printing in ETL.