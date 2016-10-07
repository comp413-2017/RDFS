// src: https://github.com/biicode/docs/blob/master/c%2B%2B/examples/gmock.rst
#pragma once
#include "turtle.h"

class Painter
{
        Turtle* turtle;
public:
        Painter( Turtle* turtle )
                :       turtle(turtle){}

        bool DrawCircle(int, int, int){
                turtle->PenDown();
                return true;
        }
};
