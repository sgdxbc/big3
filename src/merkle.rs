use bincode::{Decode, Encode};
use ring::digest;

pub type MerkleHash = [u8; 32];

#[derive(Decode, Encode)]
pub struct MerkleTree {
    nodes: Vec<Vec<MerkleHash>>,
}

impl MerkleTree {
    pub fn new(mut leaves: Vec<MerkleHash>) -> Self {
        assert!(
            !leaves.is_empty(),
            "Cannot create Merkle tree with no leaves"
        );
        if leaves.len() > 1 && !leaves.len().is_multiple_of(2) {
            // If the number of leaves is odd, duplicate the last leaf
            leaves.push(*leaves.last().unwrap());
        }

        let mut nodes = vec![leaves];
        while nodes.last().unwrap().len() > 1 {
            let parent_level = Self::build_parent_level(nodes.last().unwrap());
            nodes.push(parent_level);
        }
        MerkleTree { nodes }
    }

    fn build_parent_level(children: &[MerkleHash]) -> Vec<MerkleHash> {
        let mut parents = Vec::new();
        for chunk in children.chunks_exact(2) {
            let parent = hash_pair(&chunk[0], &chunk[1]);
            parents.push(parent);
        }
        if parents.len() > 1 && !parents.len().is_multiple_of(2) {
            // If the number of parents is odd, duplicate the last parent
            parents.push(*parents.last().unwrap());
        }
        parents
    }

    pub fn root(&self) -> MerkleHash {
        self.nodes.last().unwrap()[0]
    }
}

fn hash_pair(left: &MerkleHash, right: &MerkleHash) -> MerkleHash {
    let mut buf = [0u8; 64];
    buf[..32].copy_from_slice(left.as_ref());
    buf[32..].copy_from_slice(right.as_ref());
    digest::digest(&digest::SHA256, &buf)
        .as_ref()
        .try_into()
        .unwrap()
}

pub struct MerkleProof {
    // store each sibling and whether the sibling is on the left side of the node
    siblings: Vec<(MerkleHash, bool)>,
}

pub fn sibling_indices(
    index: usize,
    level_count: usize,
) -> impl IntoIterator<Item = (usize, bool)> {
    let mut idx = index;
    std::iter::from_fn(move || {
        let sibling_idx = idx ^ 1;
        let sibling_is_left = sibling_idx < idx;
        idx >>= 1;
        Some((sibling_idx, sibling_is_left))
    })
    .take(level_count - 1)
}

impl MerkleTree {
    pub fn prove(&self, index: usize) -> MerkleProof {
        assert!(index < self.nodes[0].len(), "Index out of bounds");
        let mut siblings = Vec::new();
        for (level, (sibling_idx, sibling_is_left)) in self
            .nodes
            .iter()
            .zip(sibling_indices(index, self.nodes.len()))
        {
            assert!(sibling_idx < level.len());
            let sibling = level[sibling_idx];
            siblings.push((sibling, sibling_is_left));
        }
        MerkleProof { siblings }
    }
}

impl MerkleProof {
    pub fn verify(&self, leaf: &MerkleHash, root: &MerkleHash) -> anyhow::Result<()> {
        let mut computed_hash = *leaf;
        for (sibling, sibling_is_left) in &self.siblings {
            if *sibling_is_left {
                computed_hash = hash_pair(sibling, &computed_hash);
            } else {
                computed_hash = hash_pair(&computed_hash, sibling);
            }
        }
        anyhow::ensure!(computed_hash == *root, "Merkle proof verification failed");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merkle_tree() {
        let leaves = (0u32..1000)
            .map(|i| {
                digest::digest(&digest::SHA256, &i.to_le_bytes())
                    .as_ref()
                    .try_into()
                    .unwrap()
            })
            .collect::<Vec<_>>();
        let tree = MerkleTree::new(leaves.clone());
        let root = tree.root();
        for (i, leaf) in leaves.iter().enumerate() {
            let proof = tree.prove(i);
            proof
                .verify(leaf, &root)
                .unwrap_or_else(|_| panic!("Proof failed for leaf {}", i));
        }
    }
}
