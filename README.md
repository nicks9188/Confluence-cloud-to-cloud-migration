# Confluence-cloud-to-cloud-migration
Confluence space migration

## System Requirement
* Python.
* Notepad.
* Any terminal application (cmd or git for windows).

## How to do the conversion?
* In the script file, replace the following values with your own data:
  * Set source
    * SRC_BASE_URL   = "<source_confluence_space_url>"     # include '/wiki'
    * SRC_SPACE_KEY  = "<source_confluence_space_key>"
    * SRC_USERNAME   = "<your_email>"
    * SRC_API_TOKEN  = "<your_PAT_token>"

  * Set destination
    * DST_BASE_URL   = "<destination_confluence_space_url>"       # include '/wiki'
    * DST_SPACE_KEY  = "<destination_confluence_space_key>"
    * DST_USERNAME   = "<your_email>"
    * DST_API_TOKEN  = "<your_PAT_token>"
   
* Run the script over the terminal. Type ```python <your_filename.py>```
* Validate in the destination if conversion is successfull.

