require 'add_this_line'
require 'fileutils'

# Process AddThisMapping files. The goal is to remove dups that come in over time, and reduce the load on the graph
# database.
#
# Each addThisMapping file is processed by being merged with the "database" file. The database file continues to grow
# as more AddThisMapping files are merged into it. Note it could be pruned by removing lines that have a timestamp > 9
# days, etc)
#
# addThisMapping file + database file => new database file + file for new mappings. 
# The database file has the mappings + tab + lastseen (timestamp). The mappings is the column with 6=1234,9=22345,1172=3223
# NOTE the addThisMapping file must be pre-processed to have mapping in first column, tab, then timestamp in second
# column. Also must be SORTED!
#
# LocalResponse cookies are ignored.
#
# Timestamps are handled by keeping the database file rolling only to 9 days.  That way we will update a cookie's
# timestamp as long as we see it within 10-30 days of putting in the mapping.  If we don't see it in that range,
# we were probably going to lose that cookie anyways.
#
# Note to preprocess the addThisMapping file, these commands can be used:
# # This command Moves column 3 to the front, then column 1
# cat "$1" | gunzip | awk '{ print $3"\t"$1 }' > mappings_"$1"
# # This command sorts the file
# java -jar ~/externalsortinginjava-0.1.10-SNAPSHOT.jar -v mappings_"$1" mappings_sorted_"$1"
# # This command runs the merge:
# bundle exec ruby -I lib run_dedup.rb mappings_sorted_"$1"
#
class DedupAddThisMerge
  DB_FILE_PATH = 'database.txt'
  DB_OUTPUT_FILE_PATH = 'database_temp_output.txt'
  #
  def initialize
  end

  def run(addthis_file_path)
    if !File.file?(DB_FILE_PATH)
      FileUtils.touch(DB_FILE_PATH)
    end

    db_file = File.open(DB_FILE_PATH, 'r')
    addthis_file = File.open(addthis_file_path, 'r')

    # TODO - Make these temp files
    db_output_file = File.open(DB_OUTPUT_FILE_PATH, 'w')
    new_mappings_file = File.open('new_mappings_' + addthis_file_path, 'w')

    db_line = AddThisLine.new(db_file.gets)
    addthis_line = AddThisLine.new(addthis_file.gets)

    while (db_line.line || addthis_line.line)
      compare = AddThisLine.compare(db_line, addthis_line)
      if compare < 0
        # Copy from database
        # TODO - Filter out lines that are older than 9 days
        db_output_file.puts(db_line.line)
        db_line = AddThisLine.new(db_file.gets)
      elsif compare > 0
        # Copy from addThis
        db_output_file.puts(addthis_line.line);
        new_mappings_file.puts(addthis_line.line);
        addthis_line = AddThisLine.new(addthis_file.gets)
      else
        # Need to merge
        # Keep the mapping from one (they are both the same), and keep the largest (most recent) timestamp
        db_timestamp = db_line.parts[1].to_i
        addthis_timestamp = addthis_line.parts[1].to_i
        max_timestamp = [db_timestamp, addthis_timestamp].max
        new_line = db_line.parts[0] + "\t" + max_timestamp.to_s
        db_output_file.puts(new_line);
        db_line = AddThisLine.new(db_file.gets)
        addthis_line = AddThisLine.new(addthis_file.gets)
      end
    end

    db_file.close
    addthis_file.close
    db_output_file.close
    new_mappings_file.close

    File.delete(DB_FILE_PATH)   # TODO:  Keep N backups just in case
    FileUtils.move(DB_OUTPUT_FILE_PATH, DB_FILE_PATH)
  end
end