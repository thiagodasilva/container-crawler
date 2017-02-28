## 0.0.2 (2017-02-28)

Bug fixes:

    - fix the handling of unicode characters in object names when logging errors
    - handler tasks no longer fail to complete if an exception is raised

Improvements:

    - logs the object name on failure (as opposed to only the row ID)
