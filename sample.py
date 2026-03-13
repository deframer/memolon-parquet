import pyarrow.parquet as pq

# Load the Parquet file
file_path = "work/memolon-grouped-de-0.1.0.parquet"
table = pq.read_table(file_path)

# Target word to find
word_to_find = "Kirsche"

# Convert the 'word' column to a list and search for the word (case-insensitive)
lowercase_word_list = [str(word).lower() for word in table.column("word").to_pylist()]
lowercase_word_to_find = word_to_find.lower()

try:
    index = lowercase_word_list.index(lowercase_word_to_find)

    # Fetch all data for that row using the found index
    row_data = {col_name: table.column(col_name)[index].as_py() for col_name in table.column_names}
    print(f"Found '{word_to_find}' (case-insensitive search):")
    for key, value in row_data.items():
        print(f"  {key}: {value}")

    print(
        "\nThese values are emotional ratings on a 1–9 scale."
        " The first three dimensions follow the VAD model (Valence, Arousal, Dominance)."
    )

    print("Scale interpretation: 1 = very low / absent, 5 = neutral / medium, 9 = very high / strong.")

    print(
        "Examples:"
        " Valence: 1 = very negative, 9 = very positive."
        " Arousal: 1 = calm / sleepy, 9 = excited / highly activated."
        " Dominance: 1 = feeling controlled or powerless, 9 = feeling in control / dominant."
    )

    print(
        "Emotion scores (Joy, Anger, Sadness, Fear, Disgust) use the same 1–9 intensity scale:"
        " 1 = emotion not present, 9 = very strong emotion."
    )

    print("Background on the VAD/PAD emotion model: https://en.wikipedia.org/wiki/PAD_emotional_state_model")

except ValueError:
    print(f"The word '{word_to_find}' was not found in the dataset.")
