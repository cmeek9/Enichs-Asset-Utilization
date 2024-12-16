import hashlib

class DataHasher:
    """
    A class for computing hashes for data integrity.
    """

    
    def compute_hash(row):
        """Compute a hash for a single row."""
        hash_str = ''.join(row.astype(str))
        return hashlib.sha256(hash_str.encode()).hexdigest()

    
    def compute_hashes(results_df):
        """Compute hash for each row in the results DataFrame."""
        results_df['hash'] = results_df.apply(DataHasher.compute_hash, axis=1)
        return results_df