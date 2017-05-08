#import std::fs::read_dir
#import std::path::Path

#import volume::Brick
#import super::GlusterError

def get_self_heal_count(brick: Brick)# -> Result<usize, GlusterError>
"""
    Find the self heal count for a given brick
"""
    brick_path = "{}/.glusterfs/indices/xattrop".format(brick.path)
    heal_path = Path::new(&brick_path)

    # Count all files that don't start with xattrop.  Those are gfid's that need healing
    let entry_count = read_dir(heal_path)
        ?
        .filter_map(|entry| entry.ok())
        .filter(|entry| !entry.file_name().to_string_lossy().starts_with("xattrop"))
        .count()
    return entry_count
