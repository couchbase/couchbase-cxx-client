# Fail loudly, at the first step of any package build, if this is not a git
# checkout. The source manifest is generated with `git ls-files`; a bare
# jujutsu (jj) workspace has no .git, which would otherwise produce an empty
# manifest and a silently broken (empty) source tarball.
# Run via: cmake -P require-git-checkout.cmake  (cwd = source tree)
# Distinguish "git missing" from "not a work tree" so each fails with an accurate message.
find_program(_git_executable git)
if(NOT _git_executable)
  message(FATAL_ERROR
    "git was not found in PATH. Package builds generate the source manifest with 'git ls-files', "
    "so git must be installed.")
endif()

execute_process(
  COMMAND "${_git_executable}" rev-parse --is-inside-work-tree
  RESULT_VARIABLE _git_result
  OUTPUT_QUIET
  ERROR_QUIET)
if(NOT _git_result EQUAL 0)
  message(FATAL_ERROR
    "Package builds require a git checkout: the source manifest is generated with "
    "'git ls-files'. If you use jujutsu (jj), build packages from the co-located git "
    "checkout (the workspace that has .git) or a git worktree -- not a bare jj workspace, "
    "which has no .git.")
endif()
