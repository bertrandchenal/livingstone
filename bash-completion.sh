
_complete () {
    local cur
    local new
    local action

    COMPREPLY=()   # Array variable storing the possible completions.
    cur=${COMP_WORDS[COMP_CWORD]}
    new=''
    case "$cur" in
        -*)
            new='-c --collect-links -e --encoding -l --length\
                   -p --page -h --help'
            COMPREPLY=($( compgen -W "$new" -- $cur ))
            ;;
        *)
            case "$COMP_CWORD" in
                "1")
                    new='add search suggest crawl info neighbours'
                    COMPREPLY=($( compgen -W "$new" -- $cur ))
                    ;;

                *)
                    action=${COMP_WORDS[1]}
                    if [[ "$action" == "add" ]]; then
                        COMPREPLY=($( compgen -f $cur ))
                    else
                        new=`lvn suggest $cur`
                        COMPREPLY=($( compgen -W "$new" -- $cur ))
                    fi
                    ;;
            esac
            ;;
    esac
    return 0
}

complete -F _complete -o filenames lvn
