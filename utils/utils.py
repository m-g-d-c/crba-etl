import nltk
import re

nltk.download("punkt")
nltk.download("stopwords")


def create_ind_code(indicator_name):
    """Create 6-digit indicator code suffic from indicator name

    Each indicator in CRBA has a name speified in the indicator dictionary. This function requires
    this indicator name and turns it into a 6-digit code. This 6-digit code can be used as suffix
    for the actual indicator code, which follows this structure:

    <2-digit index code>_<2-digit issue code>_<2-digit category code>_<6-digit indicator code>

    Indicator names may contain speial characters like "-", or "," and stopwords like "a" or "of".
    These will be deleted through this function.

    Parameters:
    indicator_name(obj): Indicator name

    Return:
    6-digit indicator code suffix.
    """

    # Remove characters
    ind_nam_charfree = re.sub("[,.-;']", "", indicator_name)

    # Tokenize indicator name
    ind_name_tok = nltk.tokenize.word_tokenize(ind_nam_charfree)

    # Remove stopwords
    in_nam_tok_ns = [
        word for word in ind_name_tok if not word in nltk.corpus.stopwords.words()
    ]

    # Create indicator code
    if len(in_nam_tok_ns) == 0:
        raise ValueError()
    elif len(in_nam_tok_ns) == 1:
        result = in_nam_tok_ns[0][0:6].upper()
    elif len(in_nam_tok_ns) == 2:
        result = in_nam_tok_ns[0][0:3].upper() + in_nam_tok_ns[1][0:3].upper()
    elif len(in_nam_tok_ns) == 3:
        result = (
            in_nam_tok_ns[0][0:2].upper()
            + in_nam_tok_ns[1][0:2].upper()
            + in_nam_tok_ns[2][0:2].upper()
        )
    elif len(in_nam_tok_ns) == 4:
        result = (
            in_nam_tok_ns[0][0:2].upper()
            + in_nam_tok_ns[1][0:2].upper()
            + in_nam_tok_ns[2][0].upper()
            + in_nam_tok_ns[3][0].upper()
        )
    elif len(in_nam_tok_ns) == 5:
        result = (
            in_nam_tok_ns[0][0:2].upper()
            + in_nam_tok_ns[1][0].upper()
            + in_nam_tok_ns[2][0].upper()
            + in_nam_tok_ns[3][0].upper()
            + in_nam_tok_ns[4][0].upper()
        )
    else:
        result = (
            in_nam_tok_ns[0][0].upper()
            + in_nam_tok_ns[1][0].upper()
            + in_nam_tok_ns[2][0].upper()
            + in_nam_tok_ns[3][0].upper()
            + in_nam_tok_ns[4][0].upper()
            + in_nam_tok_ns[5][0].upper()
        )

    # Return result
    return result
