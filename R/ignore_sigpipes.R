
#' ignore sigpipe
#'
#' Avoid SIGPIPE error in scripts using Arrow S3
#' @export
ignore_sigpipe <- function() {

  if(.Platform$OS.type == "windows") return(invisible(NULL))

  requireNamespace("decor", quietly = TRUE)
  requireNamespace("brio", quietly = TRUE)
  requireNamespace("cpp11", quietly = TRUE)

  cpp11::cpp_source(code = '
  #include <csignal>
  #include <cpp11.hpp>
  [[cpp11::register]] void ignore_sigpipes() {
    signal(SIGPIPE, SIG_IGN);
  }
  ')
  ignore_sigpipes()
}

dummy_decor <- function(pkg=".") {
  decor::cpp_decorations(pkg)
}

# Well this is mighty stupid
dummy_brio <- function() {
  requireNamespace("brio", quietly = TRUE)
  brio::write_lines("", tempfile())
}

globalVariables("ignore_sigpipes")
